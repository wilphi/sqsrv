package cmd

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

// DropTable drops a table from the database. All rows in the table are lost.
//	  This function will always return a nil dataset
func DropTable(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	var tableName string

	log.Debug("DROP TABLE command")

	// Eat the DROP TABLE tokens if they are there
	if tkns.Test(tokens.Drop) != nil {
		tkns.Remove()
	}

	if tkns.Test(tokens.Table) != nil {
		tkns.Remove()
	}

	// make sure the next token is an Ident
	if tkn := tkns.Test(tokens.Ident); tkn != nil {
		val := tkn.(*tokens.ValueToken).Value()
		tableName = strings.ToLower(val)
		tkns.Remove()
	} else {
		return "", nil, sqerr.NewSyntax("Expecting name of table to Drop")
	}

	if !tkns.IsEmpty() {
		return "", nil, sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.String())
	}

	err := sqtables.DropTable(profile, tableName)
	if err != nil {
		return "", nil, err
	}
	err = redo.Send(redo.NewDropDDL(tableName))

	return tableName, nil, err
}
