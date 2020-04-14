package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

// CreateTable - Wraps CreateTableFromTokens
func CreateTable(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	msg, err := CreateTableFromTokens(profile, tkns)

	return msg, nil, err
}

// CreateTableFromTokens - Creates a table from array of tokens that represent a CREATE TABLE statement
func CreateTableFromTokens(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (string, error) {
	var tableName string
	var cols []sqtables.ColDef

	log.Info("CREATE TABLE command")
	if tkns.Test(tokens.Create) != nil {
		tkns.Remove()
	}
	//eat TABLE tokens (Create is already gone)
	if tkns.Test(tokens.Table) != nil {
		tkns.Remove()
	}

	// make sure the next token is an Ident
	if tkn := tkns.Test(tokens.Ident); tkn != nil {
		tableName = tkn.(*tokens.ValueToken).Value()
		tkns.Remove()
	} else {
		return "", sqerr.NewSyntax("Expecting name of table to create")
	}

	if tkns.Test(tokens.OpenBracket) != nil {
		tkns.Remove()
	} else {
		return "", sqerr.NewSyntax("Expecting ( after name of table")
	}
	i := 0
	isHangingComma := false
	// loop to get the column definitions of table
	for {
		if tkns.Test(tokens.CloseBracket) != nil {
			if isHangingComma {
				return "", sqerr.NewSyntax("Unexpected \",\" before \")\"")
			}
			tkns.Remove()
			break
		}
		if i > 0 && !isHangingComma {
			return "", sqerr.NewSyntax("Comma is required to separate columns")
		}
		// Ident(colName), Ident(typeVal), opt [opt NOT, NULL],  opt comma
		if tkn := tkns.Test(tokens.Ident); tkn != nil {
			cName := tkn.(*tokens.ValueToken).Value()
			tkns.Remove()
			typeTkn := tkns.Test(tokens.AllTypes...)
			if typeTkn == nil {
				return "", sqerr.NewSyntax("Expecting column type")
			}
			tkns.Remove()
			// Check for optional NOT NULL or NULL
			isNot := false
			isNull := false
			if tkns.Test(tokens.Not) != nil {
				tkns.Remove()
				isNot = true
			}
			if tkns.Test(tokens.Null) != nil {
				tkns.Remove()
				isNull = true
			}
			if isNot && !isNull {
				// if there is a NOT there must be a NULL
				return "", sqerr.NewSyntax("Expecting a NULL after NOT in Column definition")
			}

			col := sqtables.NewColDef(cName, typeTkn.ID(), isNot)
			cols = append(cols, col)
			i++

			// check for optional comma
			if tkns.Test(tokens.Comma) != nil {
				isHangingComma = true
				tkns.Remove()
			} else {
				isHangingComma = false
			}
		} else {
			return "", sqerr.NewSyntax("Expecting name of column")
		}
	}
	if len(cols) <= 0 {
		return "", sqerr.NewSyntax("No columns defined for table")
	}

	if !tkns.IsEmpty() {
		return "", sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.String())
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
