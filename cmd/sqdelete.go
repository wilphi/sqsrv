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

// Delete -
func Delete(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	nRows, err := DeleteFromTokens(profile, tkns)

	return fmt.Sprintf("Deleted %d rows from table", nRows), nil, err
}

// DeleteFromTokens - takes a list of tokens returns number of rows deleted from table, error
func DeleteFromTokens(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (numRows int, err error) {
	var tableName string
	var td *sqtables.TableDef
	var whereExpr sqtables.Expr
	log.Info("Delete statement...")

	numRows = -1

	//eat Delete token
	tkns.Remove()

	// eat the From
	if tkns.Test(tokens.From) == "" {
		// no FROM
		err = sqerr.NewSyntax("Expecting FROM")
		return
	}
	tkns.Remove()

	//expecting Ident (tablename)
	if tableName = tkns.Test(tokens.Ident); tableName == "" {
		err = sqerr.NewSyntax("Expecting table name in Delete statement")
		return
	}
	tkns.Remove()

	// get the TableDef
	td = sqtables.GetTable(profile, tableName)
	if td == nil {
		err = sqerr.New("Table " + tableName + " does not exist for delete statement")
		return
	}

	// Optional Where clause processing goes here
	if tkns.Test(tokens.Where) != "" {
		tkns.Remove()
		whereExpr, err = GetExpr(tkns, nil, 0, tokens.Order)

		if err != nil {
			return
		}
		err = whereExpr.ValidateCols(profile, td)
		if err != nil {
			return
		}
		whereExpr, err = whereExpr.Reduce()
		if err != nil {
			return
		}

	}
	if !tkns.IsEmpty() {
		err = sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.ToString())
		return
	}

	numRows, err = DeleteFromTable(profile, tableName, whereExpr)
	return
}

// DeleteFromTable -
func DeleteFromTable(profile *sqprofile.SQProfile, tableName string, whereExpr sqtables.Expr) (numRows int, err error) {
	var rowsDeleted []int64
	numRows = -1

	tab := sqtables.GetTable(profile, tableName)
	if tab == nil {
		err = sqerr.New("Table " + tableName + " does not exist for Delete statement")
		return
	}
	rowsDeleted, err = tab.DeleteRows(profile, whereExpr)
	if err != nil {
		return
	}
	err = redo.Send(redo.NewDeleteRows(tableName, rowsDeleted))
	if err != nil {
		log.Panic("Unable to send delete command to redo")
	}

	return len(rowsDeleted), nil
}
