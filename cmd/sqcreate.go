package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/redo"
	e "github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	t "github.com/wilphi/sqsrv/tokens"
)

// CreateTable - Wraps CreateTableFromTokens
func CreateTable(profile *sqprofile.SQProfile, tkns *t.TokenList) (string, *sqtables.DataSet, error) {
	msg, err := CreateTableFromTokens(profile, tkns)

	return msg, nil, err
}

// CreateTableFromTokens - Creates a table from array of tokens that represent a CREATE TABLE statement
func CreateTableFromTokens(profile *sqprofile.SQProfile, tkns *t.TokenList) (string, error) {
	var tableName string
	var cols []sqtables.ColDef

	log.Info("CREATE TABLE command")
	if tkns.Test(t.Create) != "" {
		tkns.Remove()
	}
	//eat TABLE tokens (Create is already gone)
	if tkns.Test(t.Table) != "" {
		tkns.Remove()
	}

	// make sure the next token is an Ident
	if val := tkns.Test(t.Ident); val != "" {
		tableName = val
		tkns.Remove()
	} else {
		return "", e.NewSyntax("Expecting name of table to create")
	}

	if tkns.Test(t.OpenBracket) != "" {
		tkns.Remove()
	} else {
		return "", e.NewSyntax("Expecting ( after name of table")
	}
	i := 0
	isHangingComma := false
	// loop to get the column definitions of table
	for {
		if tkns.Test(t.CloseBracket) != "" {
			if isHangingComma {
				return "", e.NewSyntax("Unexpected \",\" before \")\"")
			}
			tkns.Remove()
			break
		}
		if i > 0 && !isHangingComma {
			return "", e.NewSyntax("Comma is required to separate column definitions")
		}
		// Ident(colName), Ident(typeVal), opt [opt NOT, NULL],  opt comma
		if cName := tkns.Test(t.Ident); cName != "" {
			tkns.Remove()
			typeVal := tkns.Test(t.TypeTKN)
			if typeVal == "" {
				return "", e.NewSyntax("Expecting column type")
			}
			tkns.Remove()
			// Check for optional NOT NULL or NULL
			isNot := false
			isNull := false
			if tkns.Test(t.Not) != "" {
				tkns.Remove()
				isNot = true
			}
			if tkns.Test(t.Null) != "" {
				tkns.Remove()
				isNull = true
			}
			if isNot && !isNull {
				// if there is a NOT there must be a NULL
				return "", e.NewSyntax("Expecting a NULL after NOT in Column definition")
			}

			col := sqtables.CreateColDef(cName, typeVal, isNot)
			cols = append(cols, col)
			i++

			// check for optional comma
			if tkns.Test(t.Comma) != "" {
				isHangingComma = true
				tkns.Remove()
			} else {
				isHangingComma = false
			}
		} else {
			return "", e.NewSyntax("Expecting name of column")
		}
	}
	if len(cols) <= 0 {
		return "", e.NewSyntax("No columns defined for table")
	}

	if !tkns.IsEmpty() {
		return "", e.NewSyntax("Unexpected tokens after SQL command:" + tkns.ToString())
	}

	log.Debug("Creating table ", tableName)
	table := sqtables.CreateTableDef(tableName, cols...)
	err := sqtables.CreateTable(profile, table)
	if err != nil {
		return "", err
	}

	err = redo.Send(redo.NewCreateDDL(tableName, cols))

	log.Trace(table)
	return tableName, err
}
