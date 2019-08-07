package cmd

import (
	"fmt"

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
	var tableCols sqtables.ColList
	var isAsterix = false
	var tableName string
	var tab *sqtables.TableDef
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

		// eat the From
		if tkns.Test(tokens.From) == "" {
			// no FROM
			return nil, sqerr.NewSyntax("Expecting FROM")
		}
		tkns.Remove()
	} else {
		// get the column list
		eList, err = GetExprList(tkns, tokens.From, false)
		if err != nil {
			return nil, err
		}
	}

	//expecting Ident (tablename)
	if tableName = tkns.Test(tokens.Ident); tableName == "" {
		return nil, sqerr.NewSyntax("Expecting table name in select statement")
	}
	tkns.Remove()

	// get the TableDef
	tab = sqtables.GetTable(profile, tableName)
	if tab == nil {
		return nil, sqerr.New("Table " + tableName + " does not exist for select statement")
	}

	// get the cols in the table
	// Once we have the table name we can generate the column list
	if isAsterix {
		tableCols = tab.GetCols(profile)
		eList = sqtables.ColsToExpr(tableCols)
	} else {
		//convert into column defs
		err = eList.ValidateCols(profile, tab)
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
			whereExpr, err = GetExpr(tkns, nil, 0, tokens.Order)
			if err != nil {
				return nil, err
			}
			whereExpr, err = whereExpr.Reduce()
			if err != nil {
				return nil, err
			}
			err = whereExpr.ValidateCols(profile, tab)
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

	return SelectExecute(profile, tableName, eList, whereExpr, orderBy)

}

// SelectExecute executes the select command against the data to return the result
func SelectExecute(
	profile *sqprofile.SQProfile,
	tableName string,
	eList *sqtables.ExprList,
	whereExpr sqtables.Expr,
	orderBy []sqtables.OrderItem) (*sqtables.DataSet, error) {

	tab := sqtables.GetTable(profile, tableName)
	if tab == nil {
		return nil, sqerr.New("Table " + tableName + " does not exist for select statement")
	}
	data, err := tab.GetRowData(profile, eList, whereExpr)
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
