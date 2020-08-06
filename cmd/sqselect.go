package cmd

import (
	"fmt"
	"sort"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/tokens"
)

// Select command with function prototype as required for dispatching
func Select(trans *sqtables.Transaction, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {

	profile := trans.Profile
	q, err := SelectParse(profile, tkns)
	if err != nil {
		return "", nil, err
	}

	data, err := SelectExecute(profile, q)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%d rows found", data.Len()), data, err
}

// SelectParse takes a list of tokens and verifies the syntax of the command
//   *sqtables.Query structure with the information required to execute the select
//   error - if !nil an error has occurred
func SelectParse(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (*sqtables.Query, error) {
	var err error

	var isAsterix = false

	log.Info("SELECT statement...")
	q := sqtables.Query{}

	//Verify and eat SELECT token
	if !tkns.IsA(tokens.Select) {
		return nil, sqerr.NewInternalf("SELECT Token not found: %s is invalid", tkns.Peek().String())
	}
	tkns.Remove()

	// check for a distinct
	if tkns.IsA(tokens.Distinct) {
		tkns.Remove()
		q.IsDistinct = true
	}
	// Get the column list. * is special case
	if tkns.IsA(tokens.Asterix) {
		tkns.Remove()
		isAsterix = true

	} else {
		// get the column list
		q.EList, err = GetExprList(tkns, tokens.From, tokens.Select)
		if err != nil {
			return nil, err
		}
	}

	// Get the FROM clause
	if !tkns.IsA(tokens.From) {
		// no FROM
		return nil, sqerr.NewSyntax("Expecting FROM")
	}
	tkns.Remove()
	//	q.Tables, err = GetTableList(profile, tkns, tokens.Where, tokens.Order, tokens.Group, tokens.Having)
	q.Tables, q.Joins, err = ParseFromClause(profile, tkns, tokens.Where, tokens.Order, tokens.Group, tokens.Having)
	if err != nil {
		return nil, err
	}
	// get the cols in the table
	// Once we have the table name we can generate the column list
	if isAsterix {
		cols := q.Tables.AllCols(profile)
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].Idx < cols[j].Idx })
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].TableName.Show() < cols[j].TableName.Show() })

		if q.Tables.Len() == 1 {
			// Only one table so alias colnames
			q.EList = sqtables.NewExprList()
			for _, col := range cols {
				colX := sqtables.NewColExpr(col)
				colX.SetAlias(col.ColName)
				q.EList.Add(colX)
			}

		} else {
			q.EList = sqtables.ColsToExpr(column.NewListRefs(cols))

		}
	} else {
		//convert into column defs
		err = q.EList.ValidateCols(profile, q.Tables)
		if err != nil {
			return nil, err
		}
	}

	// loop until no new clause is processed in a pass
	for clauseProcessed := true; clauseProcessed; {
		clauseProcessed = false
		// Optional Where clause processing goes here
		if tkns.IsA(tokens.Where) {
			clauseProcessed = true
			if q.WhereExpr != nil {
				return nil, sqerr.NewSyntax("Duplicate where clause, only one allowed")
			}
			tkns.Remove()
			q.WhereExpr, err = ParseWhereClause(tkns, false, tokens.Order, tokens.Group, tokens.Having)
			if err != nil {
				return nil, err
			}
			err = q.WhereExpr.ValidateCols(profile, q.Tables)
			if err != nil {
				return nil, err
			}
		}
		// Optional Group By Clause processing goes here
		if tkns.IsA(tokens.Group) {
			clauseProcessed = true
			if q.GroupBy != nil {
				return nil, sqerr.NewSyntax("Duplicate group by clause, only one allowed")
			}
			tkns.Remove()
			groupBy, err := GroupByClause(tkns)
			if err != nil {
				return nil, err
			}
			q.GroupBy = groupBy
			err = q.GroupBy.ValidateCols(profile, q.Tables)
			if err != nil {
				return nil, err
			}
		}

		// Optional Order By clause processing goes here
		if tkns.IsA(tokens.Order) {
			clauseProcessed = true
			if q.OrderBy != nil {
				return nil, sqerr.NewSyntax("Duplicate order by clause, only one allowed")
			}
			tkns.Remove()
			q.OrderBy, err = OrderByClause(tkns)
			if err != nil {
				return nil, err
			}
		}

		//Optional Having clause processing
		if tkns.IsA(tokens.Having) {
			clauseProcessed = true
			if q.HavingExpr != nil {
				return nil, sqerr.NewSyntax("Duplicate Having clause, only one allowed")
			}
			q.HavingExpr, err = HavingClause(tkns, tokens.Order, tokens.Group, tokens.Where)
			if err != nil {
				return nil, err
			}
		}
	}

	if !tkns.IsEmpty() {
		return nil, sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.String())
	}

	return &q, nil

}

// SelectExecute executes the select command against the data to return the result
func SelectExecute(profile *sqprofile.SQProfile, q *sqtables.Query) (*sqtables.DataSet, error) {

	data, err := q.GetRowData(profile)
	if err != nil {
		return nil, err
	}

	// If Select DISTINCT then filter out duplicates
	if q.IsDistinct {
		data.Distinct()
	}

	if q.OrderBy != nil || len(q.OrderBy) > 0 {
		err = data.SetOrder(q.OrderBy)
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
