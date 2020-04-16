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

// SelectStmt - structure to store decoded Select Statement
type SelectStmt struct {
	tkns       *tokens.TokenList
	tables     *sqtables.TableList
	isDistinct bool
	eList      *sqtables.ExprList
	whereExpr  sqtables.Expr
	orderBy    []sqtables.OrderItem
	groupBy    *sqtables.ExprList
}

// Select command with function prototype as required for dispatching
func Select(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	stmt := NewSelectStmt(tkns)
	err := stmt.SelectParse(profile)
	if err != nil {
		return "", nil, err
	}

	data, err := stmt.SelectExecute(profile)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%d rows found", data.Len()), data, err
}

// NewSelectStmt creates SelectStmt object to hold all information about query
func NewSelectStmt(tkns *tokens.TokenList) *SelectStmt {
	stmt := new(SelectStmt)
	stmt.tkns = tkns
	stmt.orderBy = nil
	return stmt
}

// SelectParse takes a list of tokens and verifies the syntax of the command
//   error - if !nil an error has occurred
func (stmt *SelectStmt) SelectParse(profile *sqprofile.SQProfile) error {
	var err error

	var isAsterix = false

	log.Info("SELECT statement...")

	//Verify and eat SELECT token
	if !stmt.tkns.IsA(tokens.Select) {
		return sqerr.NewInternalf("SELECT Token not found: %s is invalid", stmt.tkns.Peek().String())
	}
	stmt.tkns.Remove()

	// check for a distinct
	if stmt.tkns.IsA(tokens.Distinct) {
		stmt.tkns.Remove()
		stmt.isDistinct = true
	}
	// Get the column list. * is special case
	if stmt.tkns.IsA(tokens.Asterix) {
		stmt.tkns.Remove()
		isAsterix = true

	} else {
		// get the column list
		stmt.eList, err = GetExprList(stmt.tkns, tokens.From, tokens.Select)
		if err != nil {
			return err
		}
	}

	// Get the FROM clause
	if !stmt.tkns.IsA(tokens.From) {
		// no FROM
		return sqerr.NewSyntax("Expecting FROM")
	}
	stmt.tkns.Remove()
	stmt.tables, err = GetTableList(profile, stmt.tkns, tokens.Where, tokens.Order, tokens.Group)
	if err != nil {
		return err
	}
	// get the cols in the table
	// Once we have the table name we can generate the column list
	if isAsterix {
		cols := stmt.tables.AllCols(profile)
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].Idx < cols[j].Idx })
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].TableName < cols[j].TableName })

		if stmt.tables.Len() == 1 {
			// Only one table so alias colnames
			stmt.eList = sqtables.NewExprList()
			for _, col := range cols {
				colX := sqtables.NewColExpr(col)
				colX.SetAlias(col.ColName)
				stmt.eList.Add(colX)
			}

		} else {
			stmt.eList = sqtables.ColsToExpr(sqtables.NewColListDefs(cols))

		}
	} else {
		//convert into column defs
		err = stmt.eList.ValidateCols(profile, stmt.tables)
		if err != nil {
			return err
		}
	}

	// loop twice just in case the where clause is after the order by clause
	for i := 0; i < 2; i++ {
		// Optional Where clause processing goes here
		if stmt.tkns.IsA(tokens.Where) {
			if stmt.whereExpr != nil {
				return sqerr.NewSyntax("Duplicate where clause, only one allowed")
			}
			stmt.tkns.Remove()
			stmt.whereExpr, err = ParseWhereClause(stmt.tkns, tokens.Order, tokens.Group)
			if err != nil {
				return err
			}
			err = stmt.whereExpr.ValidateCols(profile, stmt.tables)
			if err != nil {
				return err
			}
		}
		// Optional Group By Clause processing goes here
		if stmt.tkns.IsA(tokens.Group) {
			if stmt.groupBy != nil {
				return sqerr.NewSyntax("Duplicate group by clause, only one allowed")
			}
			stmt.tkns.Remove()
			groupBy, err := GroupByClause(stmt.tkns)
			if err != nil {
				return err
			}
			stmt.groupBy = groupBy
			err = stmt.groupBy.ValidateCols(profile, stmt.tables)
			if err != nil {
				return err
			}
		}

		// Optional Order By clause processing goes here
		if stmt.tkns.IsA(tokens.Order) {
			if stmt.orderBy != nil {
				return sqerr.NewSyntax("Duplicate order by clause, only one allowed")
			}
			stmt.tkns.Remove()
			stmt.orderBy, err = OrderByClause(stmt.tkns)
			if err != nil {
				return err
			}
		}
	}

	if !stmt.tkns.IsEmpty() {
		return sqerr.NewSyntax("Unexpected tokens after SQL command:" + stmt.tkns.String())
	}

	return nil

}

// SelectExecute executes the select command against the data to return the result
func (stmt *SelectStmt) SelectExecute(profile *sqprofile.SQProfile) (*sqtables.DataSet, error) {

	data, err := stmt.tables.GetRowData(profile, stmt.eList, stmt.whereExpr, stmt.groupBy)
	if err != nil {
		return nil, err
	}

	// If Select DISTINCT then filter out duplicates
	if stmt.isDistinct {
		data.Distinct()
	}

	if stmt.orderBy != nil || len(stmt.orderBy) > 0 {
		err = data.SetOrder(stmt.orderBy)
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
