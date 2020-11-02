package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/tokens"
)

// CreateTableStmt -
type CreateTableStmt struct {
	TableName   string
	Cols        []column.Def
	Constraints []sqtables.Constraint
}

// CreateTable - Wraps CreateTableFromTokens
func CreateTable(trans sqtables.Transaction, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {

	if !trans.Auto() {
		return "", nil, sqerr.New("DDL statements cannot be executed within a transaction")
	}

	tableStmt, err := ParseCreateTable(trans, tkns)
	if err != nil {
		return "", nil, err
	}
	//	msg, err := CreateTableFromTokens(profile, tkns)
	msg, err := executeCreateTable(trans, tableStmt)

	return msg, nil, err
}

// ParseCreateTable - Creates a table from array of tokens that represent a CREATE TABLE statement
func ParseCreateTable(trans sqtables.Transaction, tkns *tokens.TokenList) (*CreateTableStmt, error) {
	var err error
	var stmt CreateTableStmt

	log.Debug("CREATE TABLE command")
	tkns.IsARemove(tokens.Create)

	//eat TABLE tokens (Create is already gone)
	tkns.IsARemove(tokens.Table)

	// make sure the next token is an Ident
	if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
		stmt.TableName = tkn.(*tokens.ValueToken).Value()
		tkns.Remove()
	} else {
		return nil, sqerr.NewSyntax("Expecting name of table to create")
	}

	if !tkns.IsARemove(tokens.OpenBracket) {
		return nil, sqerr.NewSyntax("Expecting ( after name of table")
	}
	i := 0
	isHangingComma := false
	// loop to get the column definitions of table
	for {
		if tkns.IsARemove(tokens.CloseBracket) {
			if isHangingComma {
				return nil, sqerr.NewSyntax("Unexpected \",\" before \")\"")
			}
			break
		}
		if i > 0 && !isHangingComma {
			return nil, sqerr.NewSyntax("Comma is required to separate columns")
		}
		// Ident(colName), Ident(typeVal), opt [opt NOT, NULL],  opt comma
		if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
			cName := tkn.(*tokens.ValueToken).Value()
			tkns.Remove()
			typeTkn := tkns.TestTkn(tokens.AllTypes...)
			if typeTkn == nil {
				return nil, sqerr.NewSyntax("Expecting column type")
			}
			tkns.Remove()
			// Check for optional NOT NULL or NULL
			isNot := tkns.IsARemove(tokens.Not)

			isNull := tkns.IsARemove(tokens.Null)

			if isNot && !isNull {
				// if there is a NOT there must be a NULL
				return nil, sqerr.NewSyntax("Expecting a NULL after NOT in Column definition")
			}

			col := column.NewDef(cName, typeTkn.ID(), isNot)
			stmt.Cols = append(stmt.Cols, col)
			i++

			// check for optional comma
			isHangingComma = tkns.IsARemove(tokens.Comma)

		} else {
			return nil, sqerr.NewSyntax("Expecting name of column")
		}
	}
	if len(stmt.Cols) <= 0 {
		return nil, sqerr.NewSyntax("No columns defined for table")
	}

	if tkns.IsARemove(tokens.Comma) {
		stmt.Constraints, err = constraintClauses(tkns)
		if err != nil {
			return nil, err
		}
	}
	if !tkns.IsEmpty() {
		return nil, sqerr.NewSyntax("Unexpected tokens after SQL command:" + tkns.String())
	}

	return &stmt, nil
}

func executeCreateTable(trans sqtables.Transaction, stmt *CreateTableStmt) (string, error) {

	log.Debug("Creating table ", stmt.TableName)
	table := sqtables.CreateTableDef(stmt.TableName, stmt.Cols)
	err := table.AddConstraints(trans.Profile(), stmt.Constraints)
	if err != nil {
		return "", err
	}
	err = sqtables.CreateTable(trans.Profile(), table)
	if err != nil {
		return "", err
	}

	//	err = redo.Send(redo.NewCreateDDL(ableName, cols))

	log.Trace(table)
	return stmt.TableName, err
}

func constraintClauses(tkns *tokens.TokenList) ([]sqtables.Constraint, error) {
	var cons []sqtables.Constraint
	var name string

	isHangingComma := true
	// loop until no new clause is processed in a pass
	for isHangingComma {
		if tkns.IsEmpty() {
			break
		}
		switch tkns.Peek().ID() {
		case tokens.Primary:
			//Process Primary Key constraint
			tkns.Remove()
			if !tkns.IsARemove(tokens.Key) {
				return nil, sqerr.NewSyntaxf("Table constraint missing keyword KEY after PRIMARY")
			}
			if !tkns.IsARemove(tokens.OpenBracket) {
				return nil, sqerr.NewSyntax("Expecting ( after PRIMARY KEY")
			}
			cols, err := GetIdentList(tkns, tokens.CloseBracket)
			if err != nil {
				return nil, err
			}
			cons = append(cons, sqtables.NewPrimaryKey(cols))
		case tokens.Unique:
			// Process Unique constraint
			tkns.Remove()
			tkn := tkns.TestTkn(tokens.Ident)
			if tkn == nil {
				return nil, sqerr.NewSyntax("Missing a name for the Unique constraint")
			}
			name = tkn.(*tokens.ValueToken).Value()
			tkns.Remove()
			if !tkns.IsARemove(tokens.OpenBracket) {
				return nil, sqerr.NewSyntax("Expecting ( after name of constraint")
			}
			cols, err := GetIdentList(tkns, tokens.CloseBracket)
			if err != nil {
				return nil, err
			}
			cons = append(cons, sqtables.NewUnique(name, cols))
		case tokens.Foreign:
			// Process Foreign Key constraint
			tkns.Remove()
			if !tkns.IsARemove(tokens.Key) {
				return nil, sqerr.NewSyntaxf("Missing keyword KEY after FOREIGN")
			}
			tkn := tkns.TestTkn(tokens.Ident)
			if tkn == nil {
				return nil, sqerr.NewSyntax("Missing a name for the Foreign Key constraint")
			}
			name = tkn.(*tokens.ValueToken).Value()
			tkns.Remove()

			if !tkns.IsARemove(tokens.OpenBracket) {
				return nil, sqerr.NewSyntax("Expecting ( after name of constraint")
			}
			cols, err := GetIdentList(tkns, tokens.CloseBracket)
			if err != nil {
				return nil, err
			}
			cons = append(cons, sqtables.NewForeignKey(name, cols))
		case tokens.Index:
			// Process Index
			tkns.Remove()

			// get index definition
			return nil, sqerr.NewSyntax("Index Constraint not fully implemented")
		default:
			return nil, sqerr.NewSyntaxf("Unexpected tokens after comma - %s", tkns.String())
		}
		// check for optional comma
		if tkns.IsA(tokens.Comma) {
			isHangingComma = true
			tkns.Remove()
		} else {
			isHangingComma = false
		}
	}
	if isHangingComma {
		return nil, sqerr.NewSyntax("Expecting a constraint clause (Primary Key, Foreign, Index, Unique) after comma")
	}
	return cons, nil
}
