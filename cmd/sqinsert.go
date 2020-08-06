package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// InsertStmt - structure to store decoded Insert Statement
type InsertStmt struct {
	tkns      *tokens.TokenList
	tableName string
	data      *sqtables.DataSet
}

// InsertInto -
func InsertInto(trans *sqtables.Transaction, tl *tokens.TokenList) (string, *sqtables.DataSet, error) {

	ins := InsertStmt{tkns: tl}

	err := ins.Parse(trans.Profile)
	if err != nil {
		return "Zero rows inserted", nil, err
	}
	i, err := ins.executeInsert(trans)

	if trans.Auto() {
		if err != nil {
			trans.Rollback()
			return fmt.Sprintf("%d rows inserted into %s", i, ins.tableName), nil, err
		}
		err = trans.Commit()
	}
	return fmt.Sprintf("%d rows inserted into %s", i, ins.tableName), nil, err
}

// Parse translates the command string into an internal representation of the insert statment
func (ins *InsertStmt) Parse(profile *sqprofile.SQProfile) error {
	var err error
	var colNames []string
	log.Debug("Parsing INSERT INTO statement....")

	// make that this is an Insert
	if !ins.tkns.IsARemove(tokens.Insert) {
		return sqerr.New("Expecting INSERT INTO to start the statement")
	}
	if !ins.tkns.IsARemove(tokens.Into) {
		return sqerr.New("Expecting INSERT INTO to start the statement")
	}
	// make sure the next token is an Ident - TableName
	if tkn := ins.tkns.TestTkn(tokens.Ident); tkn != nil {
		ins.tableName = tkn.(*tokens.ValueToken).Value()
		ins.tkns.Remove()
	} else {
		return sqerr.NewSyntax("Expecting name of table for insert")
	}

	if !ins.tkns.IsARemove(tokens.OpenBracket) {
		return sqerr.NewSyntax("Expecting ( after name of table")
	}

	colNames, err = GetIdentList(ins.tkns, tokens.CloseBracket)
	if err != nil {
		return err
	}
	tab, err := sqtables.GetTable(profile, ins.tableName)
	if err != nil {
		return err
	}
	if tab == nil {
		return sqerr.New("Table " + ins.tableName + " does not exist")
	}

	ins.data, err = sqtables.NewDataSet(profile, sqtables.NewTableListFromTableDef(profile, tab), sqtables.ColsToExpr(column.NewListNames(colNames)))
	if err != nil {
		return err
	}

	//Values section
	err = ins.getInsertValues()
	if err != nil {
		return err
	}

	if ins.tkns.Len() != 0 {
		return sqerr.NewSyntaxf("Unexpected tokens after the values section: %s", ins.tkns.String())
	}
	return nil

}

// parse the values clause of the insert statement
func (ins *InsertStmt) getInsertValues() error {

	var vals []sqtypes.Value
	var err error

	if !ins.tkns.IsARemove(tokens.Values) {
		return sqerr.NewSyntax("Expecting keyword VALUES")
	}

	if !ins.tkns.IsA(tokens.OpenBracket) {
		return sqerr.NewSyntax("Expecting ( after keyword VALUES")
	}

	for {
		vals, err = ins.getValuesRow()
		if err != nil {
			return err
		}
		ins.data.Vals = append(ins.data.Vals, vals)

		if !ins.tkns.IsARemove(tokens.Comma) {
			// If not a comma then we are done processing VALUES clause
			break
		}
	}
	return nil
}

// parse an individual row in the Values clause
func (ins *InsertStmt) getValuesRow() ([]sqtypes.Value, error) {
	var vals []sqtypes.Value
	vals = make([]sqtypes.Value, ins.data.NumCols())

	if !ins.tkns.IsARemove(tokens.OpenBracket) {
		return nil, sqerr.NewSyntax("Expecting ( to start next row of VALUES")
	}

	eList, err := GetExprList(ins.tkns, tokens.CloseBracket, tokens.Values)
	if err != nil {
		return nil, err
	}
	if !ins.tkns.IsARemove(tokens.CloseBracket) {
		return nil, sqerr.NewSyntax("Expecting ) to finish row of VALUES")
	}

	vals, err = eList.GetValues()
	return vals, err
}

func (ins *InsertStmt) executeInsert(trans *sqtables.Transaction) (int, error) {
	// make sure there is a valid table
	tab, err := sqtables.GetTable(trans.Profile, ins.tableName)
	if err != nil {
		return 0, err
	}
	if tab == nil {
		return 0, sqerr.New("Table " + ins.tableName + " does not exist")
	}

	nRows, err := tab.AddRows(trans, ins.data)
	if err != nil {
		return 0, err
	}
	//	err = redo.Send(redo.NewInsertRows(ins.tableName, ins.data.GetColNames(), ins.data.Vals, ins.data.Ptrs))
	return nRows, err
}
