package cmd

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

// DropTable drops a table from the database. All rows in the table are lost.
//	  This function will always return a nil dataset
func DropTable(trans *sqtables.Transaction, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	var tableName string

	if !trans.Auto() {
		return "", nil, sqerr.New("DDL statements cannot be executed within a transaction")
	}

	log.Debug("DROP TABLE command")

	// Eat the DROP TABLE tokens if they are there
	tkns.IsARemove(tokens.Drop)

	tkns.IsARemove(tokens.Table)

	// make sure the next token is an Ident
	if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
		val := tkn.(*tokens.ValueToken).Value()
		tableName = strings.ToLower(val)
		tkns.Remove()
	} else {
		return "", nil, sqerr.NewSyntax("Expecting name of table to Drop")
	}

	if !tkns.IsEmpty() {
		return "", nil, sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.String())
	}

	err := sqtables.DropTable(trans.Profile, tableName)
	if err != nil {
		return "", nil, err
	}
	//	err = redo.Send(redo.NewDropDDL(tableName))

	return tableName, nil, err
}
