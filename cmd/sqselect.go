package cmd

import (
	"fmt"
	"sort"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

// Select command with function prototype as required for dispatching
func Select(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	data, err := SelectParse(profile, tkns)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%d rows found", data.Len()), data, err
}

// SelectParse takes a list of tokens and verifies the syntax of the command
//	 DataSet - All data found by select statement
//   error - if !nil an error has occurred
func SelectParse(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (*sqtables.DataSet, error) {
	var err error
	//var colNames []string
	var eList *sqtables.ExprList
	//var tableCols sqtables.ColList
	var isAsterix = false
	var tables *sqtables.TableList
	var whereExpr sqtables.Expr
	var orderBy []sqtables.OrderItem

	orderBy = nil

	log.Info("SELECT statement...")

	//eat SELECT token
	tkns.Remove()

	// Get the column list. * is special case
	if tkns.Test(tokens.Asterix) != "" {
		tkns.Remove()
		isAsterix = true

	} else {
		// get the column list
		eList, err = GetExprList(tkns, tokens.From, false)
		if err != nil {
			return nil, err
		}
	}

	// Get the FROM clause
	if tkns.Test(tokens.From) == "" {
		// no FROM
		return nil, sqerr.NewSyntax("Expecting FROM")
	}
	tkns.Remove()
	tables, err = GetTableList(profile, tkns, tokens.Where, tokens.Order)
	if err != nil {
		return nil, err
	}
	// get the cols in the table
	// Once we have the table name we can generate the column list
	if isAsterix {
		cols := tables.AllCols(profile)
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].Idx < cols[j].Idx })
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].TableName < cols[j].TableName })

		if tables.Len() == 1 {
			// Only one table so alias colnames
			eList = sqtables.NewExprList()
			for _, col := range cols {
				colX := sqtables.NewColExpr(col)
				colX.SetAlias(col.ColName)
				eList.Add(colX)
			}

		} else {
			eList = sqtables.ColsToExpr(sqtables.NewColListDefs(cols))

		}
	} else {
		//convert into column defs
		err = eList.ValidateCols(profile, tables)
		if err != nil {
			return nil, err
		}
	}

	whereProcessed := false
	orderByProcessed := false
	// loop twice just in case the where clause is after the order by clause
	for i := 0; i < 2; i++ {
		// Optional Where clause processing goes here
		if tkns.Test(tokens.Where) != "" {
			if whereProcessed {
				return nil, sqerr.NewSyntax("Duplicate where clause, only one allowed")
			}
			whereProcessed = true
			tkns.Remove()
			whereExpr, err = ParseWhereClause(tkns, tokens.Order)
			if err != nil {
				return nil, err
			}
			err = whereExpr.ValidateCols(profile, tables)
			if err != nil {
				return nil, err
			}
		}

		// Optional Order By clause processing goes here
		if tkns.Test(tokens.Order) != "" {
			if orderByProcessed {
				return nil, sqerr.NewSyntax("Duplicate order by clause, only one allowed")
			}
			orderByProcessed = true
			tkns.Remove()
			orderBy, err = OrderByClause(tkns)
			if err != nil {
				return nil, err
			}
		}
	}

	if !tkns.IsEmpty() {
		return nil, sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.ToString())
	}

	return SelectExecute(profile, tables, eList, whereExpr, orderBy)

}

// SelectExecute executes the select command against the data to return the result
func SelectExecute(
	profile *sqprofile.SQProfile,
	tables *sqtables.TableList,
	eList *sqtables.ExprList,
	whereExpr sqtables.Expr,
	orderBy []sqtables.OrderItem) (*sqtables.DataSet, error) {

	data, err := tables.GetRowData(profile, eList, whereExpr)
	if err != nil {
		return nil, err
	}
	if orderBy != nil || len(orderBy) > 0 {
		err = data.SetOrder(orderBy)
		if err != nil {
			return nil, err
		}
		err = data.Sort()
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}
