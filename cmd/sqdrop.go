package cmd

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/redo"
	e "github.com/wilphi/sqsrv/sqerr"
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
	if tkns.Test(tokens.Drop) != "" {
		tkns.Remove()
	}

	if tkns.Test(tokens.Table) != "" {
		tkns.Remove()
	}

	// make sure the next token is an Ident
	if val := tkns.Test(tokens.Ident); val != "" {
		tableName = strings.ToLower(val)
		tkns.Remove()
	} else {
		return tableName, nil, e.NewSyntax("Expecting name of table to Drop")
	}

	err := sqtables.DropTable(profile, tableName)
	if err != nil {
		return "", nil, err
	}
	err = redo.Send(redo.NewDropDDL(tableName))

	return tableName, nil, err
}
