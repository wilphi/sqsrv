package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

// Update implements the SQL command UPDATE
func Update(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	var err error
	var tableName, colName string
	var setCols []string
	var setExprs sqtables.ExprList
	var whereExpr sqtables.Expr

	colCheck := make(map[string]bool)

	log.Info("Update statement...")

	// Eat Update Token
	if tkns.Test(tokens.Update) != "" {
		tkns.Remove()
	}

	//expecting Ident (tablename)
	if tableName = tkns.Test(tokens.Ident); tableName == "" {
		return "", nil, sqerr.NewSyntax("Expecting table name in Update statement")
	}
	tkns.Remove()
	tab := sqtables.GetTable(profile, tableName)
	if tab == nil {
		return "", nil, sqerr.NewSyntaxf("Invalid table name: %s does not exist", tableName)
	}

	// eat the SET
	if tkns.Test(tokens.Set) == "" {
		// no SET
		return "", nil, sqerr.NewSyntax("Expecting SET")
	}
	tkns.Remove()
	isValidSetExpression := false
	// col = value
	for {
		// stop if end of tokens or a WHERE
		if tkns.Len() <= 0 || tkns.Test(tokens.Where) != "" {
			break
		}
		// Identifier first
		if colName = tkns.Test(tokens.Ident); colName != "" {
			cd := tab.FindColDef(profile, colName)
			if cd == nil {
				return "", nil, sqerr.NewSyntaxf("Invalid Column name: %s does not exist in Table %s", colName, tableName)
			}
			tkns.Remove()
			// Then an EQUAL sign
			if tkns.Test(tokens.Equal) == "" {
				return "", nil, sqerr.NewSyntaxf("Expecting = after column name %s in UPDATE SET", colName)
			}
			tkns.Remove()

			// Get a value/expression
			ex, err := GetExpr(tkns, nil, 0, tokens.Where, tokens.Comma)
			if err != nil {
				return "", nil, err
			}
			if ex == nil {
				return "", nil, sqerr.NewSyntaxf("Expecting an expression in SET clause after %s =", colName)
			}
			if _, ok := colCheck[colName]; ok {
				return "", nil, sqerr.NewSyntaxf("%s is set more than once", colName)
			}
			colCheck[colName] = true
			setCols = append(setCols, colName)
			setExprs.Add(ex)
			isValidSetExpression = true
			if tkns.Test(tokens.Comma) != "" {
				tkns.Remove()
			} else {
				break
			}
			/*
				} else {
					err = sqerr.NewSyntaxf("Expecting a value in SET clause after %s =", colName))
					return "", nil, err
				} */
		}

	}
	if !isValidSetExpression {
		return "", nil, sqerr.NewSyntax("Expecting valid SET expression")
	}
	// Optional Where Clause
	if tkns.Len() > 0 && tkns.Test(tokens.Where) != "" {
		tkns.Remove()
		whereExpr, err = GetExpr(tkns, nil, 0)
		if err != nil {
			return "", nil, err
		}
		whereExpr, err = whereExpr.Reduce()
		if err != nil {
			return "", nil, err
		}
		err = whereExpr.ValidateCols(profile, tab)
		if err != nil {
			return "", nil, err
		}
	}

	if !tkns.IsEmpty() {
		return "", nil, sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.ToString())
	}

	err = setExprs.ValidateCols(profile, tab)
	if err != nil {
		return "", nil, err
	}
	// get the data
	tab.Lock(profile)
	defer tab.Unlock(profile)
	ptrs, err := tab.GetRowPtrs(profile, whereExpr, false)
	if err != nil {
		return "", nil, err
	}

	//Update the rows
	err = tab.UpdateRowsFromPtrs(profile, ptrs, setCols, &setExprs)
	if err != nil {
		return "", nil, err
	}
	err = redo.Send(redo.NewUpdateRows(tableName, setCols, &setExprs, ptrs))

	return fmt.Sprintf("Updated %d rows from table", len(ptrs)), nil, err
}
