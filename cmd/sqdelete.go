package cmd

import (
	"fmt"

	"github.com/wilphi/sqsrv/sqptr"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

// Delete -
func Delete(trans sqtables.Transaction, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	tab, whereExpr, err := ParseDelete(trans, tkns)
	if err != nil {
		return "", nil, err
	}
	nRows, err := ExecuteDelete(trans, tab, whereExpr)
	return fmt.Sprintf("Deleted %d rows from table", nRows), nil, err
}

// ParseDelete - takes a list of tokens returns number of rows deleted from table, error
func ParseDelete(trans sqtables.Transaction, tkns *tokens.TokenList) (*sqtables.TableDef, sqtables.Expr, error) {
	var tableName string
	var tab *sqtables.TableDef
	var whereExpr sqtables.Expr
	var tkn tokens.Token
	var err error

	log.Debug("Delete statement...")

	//eat Delete token
	if !tkns.IsARemove(tokens.Delete) {
		return nil, nil, sqerr.NewSyntax("Expecting DELETE")
	}

	// eat the From
	if !tkns.IsARemove(tokens.From) {
		// no FROM
		return nil, nil, sqerr.NewSyntax("Expecting FROM")
	}

	//expecting Ident (tablename)
	if tkn = tkns.TestTkn(tokens.Ident); tkn == nil {
		return nil, nil, sqerr.NewSyntax("Expecting table name in Delete statement")
	}
	tableName = tkn.(*tokens.ValueToken).Value()
	tkns.Remove()

	// get the TableDef
	tab, err = sqtables.GetTable(trans.Profile(), tableName)
	if err != nil {
		return nil, nil, err
	}
	if tab == nil {
		return nil, nil, sqerr.New("Table " + tableName + " does not exist for delete statement")
	}

	// Optional Where clause processing goes here
	if tkns.IsARemove(tokens.Where) {
		whereExpr, err = ParseWhereClause(tkns, false, tokens.Order)

		if err != nil {
			return nil, nil, err
		}
		err = whereExpr.ValidateCols(trans.Profile(), sqtables.NewTableListFromTableDef(trans.Profile(), tab))
		if err != nil {
			return nil, nil, err
		}

	}
	if !tkns.IsEmpty() {
		return nil, nil, sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.String())
	}

	return tab, whereExpr, nil
}

// ExecuteDelete -
func ExecuteDelete(trans sqtables.Transaction, tab *sqtables.TableDef, whereExpr sqtables.Expr) (numRows int, err error) {
	var rowsDeleted sqptr.SQPtrs
	numRows = -1

	rowsDeleted, err = tab.DeleteRows(trans, whereExpr)
	if err != nil {
		return
	}
	/*
		err = redo.Send(redo.NewDeleteRows(tableName, rowsDeleted))
		if err != nil {
			log.Panic("Unable to send delete command to redo")
		}
	*/
	return len(rowsDeleted), nil
}
