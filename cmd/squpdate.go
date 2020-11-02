package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

//UpdateStmt contains the info required to execute an UPDATE statement
type UpdateStmt struct {
	TableName string
	Table     *sqtables.TableDef
	SetCols   []string
	SetExprs  sqtables.ExprList
	WhereExpr sqtables.Expr
}

// Update implements the SQL command UPDATE
func Update(trans sqtables.Transaction, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	stmt, err := parseUpdate(trans, tkns)
	if err != nil {
		return "", nil, err
	}
	msg, err := executeUpdate(trans, stmt)
	return msg, nil, err
}

func parseUpdate(trans sqtables.Transaction, tkns *tokens.TokenList) (*UpdateStmt, error) {
	var err error
	var stmt UpdateStmt

	colCheck := make(map[string]bool)

	log.Debug("Update statement...")

	// Eat Update Token
	tkns.IsARemove(tokens.Update)

	//expecting Ident (tablename)
	tkn := tkns.TestTkn(tokens.Ident)
	if tkn == nil {
		return nil, sqerr.NewSyntax("Expecting table name in Update statement")
	}
	stmt.TableName = tkn.(*tokens.ValueToken).Value()

	tkns.Remove()
	stmt.Table, err = sqtables.GetTable(trans.Profile(), stmt.TableName)
	if err != nil {
		return nil, err
	}
	if stmt.Table == nil {
		return nil, sqerr.NewSyntaxf("Invalid table name: %s does not exist", stmt.TableName)
	}

	// eat the SET
	if !tkns.IsARemove(tokens.Set) {
		// no SET
		return nil, sqerr.NewSyntax("Expecting SET")
	}

	isValidSetExpression := false
	// col = value
	for {
		// stop if end of tokens or a WHERE
		if tkns.Len() <= 0 || tkns.IsA(tokens.Where) {
			break
		}
		// Identifier first
		if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
			colName := tkn.(*tokens.ValueToken).Value()
			cd := stmt.Table.FindColDef(trans.Profile(), colName)
			if cd == nil {
				return nil, sqerr.NewSyntaxf("Invalid Column name: %s does not exist in Table %s", colName, stmt.TableName)
			}
			tkns.Remove()
			// Then an EQUAL sign
			if !tkns.IsA(tokens.Equal) {
				return nil, sqerr.NewSyntaxf("Expecting = after column name %s in UPDATE SET", colName)
			}
			tkns.Remove()

			// Get a value/expression
			ex, err := GetExpr(tkns, nil, 0, tokens.Where, tokens.Comma)
			if err != nil {
				return nil, err
			}
			if ex == nil {
				return nil, sqerr.NewSyntaxf("Expecting an expression in SET clause after %s =", colName)
			}
			if _, ok := colCheck[colName]; ok {
				return nil, sqerr.NewSyntaxf("%s is set more than once", colName)
			}
			colCheck[colName] = true
			stmt.SetCols = append(stmt.SetCols, colName)
			stmt.SetExprs.Add(ex)
			isValidSetExpression = true
			if tkns.IsA(tokens.Comma) {
				tkns.Remove()
			} else {
				break
			}
		}

	}
	if !isValidSetExpression {
		return nil, sqerr.NewSyntax("Expecting valid SET expression")
	}
	// Optional Where Clause
	if tkns.Len() > 0 && tkns.IsA(tokens.Where) {
		tkns.Remove()
		stmt.WhereExpr, err = ParseWhereClause(tkns, false)
		if err != nil {
			return nil, err
		}
		err = stmt.WhereExpr.ValidateCols(trans.Profile(), sqtables.NewTableListFromTableDef(trans.Profile(), stmt.Table))
		if err != nil {
			return nil, err
		}
	}

	if !tkns.IsEmpty() {
		return nil, sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.String())
	}

	return &stmt, nil
}

func executeUpdate(trans sqtables.Transaction, stmt *UpdateStmt) (string, error) {
	err := stmt.SetExprs.ValidateCols(trans.Profile(), sqtables.NewTableListFromTableDef(trans.Profile(), stmt.Table))
	if err != nil {
		return "", err
	}
	l, err := stmt.Table.UpdateRows(trans, stmt.WhereExpr, stmt.SetCols, &stmt.SetExprs)
	//err = redo.Send(redo.NewUpdateRows(tableName, setCols, &setExprs, ptrs))

	return fmt.Sprintf("Updated %d rows from table", l), err
}
