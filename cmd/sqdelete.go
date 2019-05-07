package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/redo"
	e "github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	t "github.com/wilphi/sqsrv/tokens"
)

// Delete -
func Delete(profile *sqprofile.SQProfile, tkns *t.TokenList) (string, *sqtables.DataSet, error) {
	nRows, err := DeleteFromTokens(profile, tkns)

	return fmt.Sprintf("Deleted %d rows from table", nRows), nil, err
}

// DeleteFromTokens - takes a list of tokens returns number of rows deleted from table, error
func DeleteFromTokens(profile *sqprofile.SQProfile, tkns *t.TokenList) (int, error) {
	var err error
	var tableName string
	var td *sqtables.TableDef
	var whereConditions sqtables.Condition
	log.Info("Delete statement...")

	//eat Delete token
	tkns.Remove()

	// eat the From
	if tkns.Test(t.From) == "" {
		// no FROM
		return -1, e.NewSyntax("Expecting FROM")
	}
	tkns.Remove()

	//expecting Ident (tablename)
	if tableName = tkns.Test(t.Ident); tableName == "" {
		return -1, e.NewSyntax("Expecting table name in Delete statement")
	}
	tkns.Remove()

	// get the TableDef
	td = sqtables.GetTable(profile, tableName)
	if td == nil {
		return -1, e.New("Table " + tableName + " does not exist for delete statement")
	}

	// Optional Where clause processing goes here
	if tkns.Test(t.Where) != "" {
		tkns.Remove()
		tkns, whereConditions, err = GetWhereConditions(profile, tkns, td)
		if err != nil {
			return -1, err
		}
	}

	return DeleteFromTable(profile, tableName, whereConditions)
}

// DeleteFromTable -
func DeleteFromTable(profile *sqprofile.SQProfile, tableName string, whereConditions sqtables.Condition) (int, error) {
	t := sqtables.GetTable(profile, tableName)
	if t == nil {
		return -1, e.New("Table " + tableName + " does not exist for Delete statement")
	}
	rowsDeleted, err := t.DeleteRows(profile, whereConditions)
	if err != nil {
		return -1, err
	}
	err = redo.Send(redo.NewDeleteRows(tableName, rowsDeleted))

	return len(rowsDeleted), nil
}
