package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
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
func InsertInto(profile *sqprofile.SQProfile, tl *tokens.TokenList) (string, *sqtables.DataSet, error) {
	ins, err := NewInsertStmt(tl)
	if err != nil {
		return "Zero rows inserted", nil, err
	}
	err = ins.Decode(profile)
	if err != nil {
		return "Zero rows inserted", nil, err
	}
	i, err := ins.insertIntoTables(profile)
	return fmt.Sprintf("%d rows inserted into %s", i, ins.tableName), nil, err
}

// NewInsertStmt - Create the Insert Statement
func NewInsertStmt(tl *tokens.TokenList) (*InsertStmt, error) {

	// make that this is an Insert
	if tl.Test(tokens.Insert) == "" {
		return nil, sqerr.New("Expecting INSERT INTO to start the statement")
	}
	tl.Remove()
	if tl.Test(tokens.Into) == "" {
		return nil, sqerr.New("Expecting INSERT INTO to start the statement")
	}
	tl.Remove()
	ins := InsertStmt{tkns: tl}
	return &ins, nil
}

// Decode - decodes the insert statment
func (ins *InsertStmt) Decode(profile *sqprofile.SQProfile) error {
	var err error
	var colNames []string
	log.Debug("Decoding INSERT INTO statement....")

	// make sure the next token is an Ident - TableName
	if val := ins.tkns.Test(tokens.Ident); val != "" {
		ins.tableName = val
		ins.tkns.Remove()
	} else {
		return sqerr.NewSyntax("Expecting name of table for insert")
	}

	if ins.tkns.Test(tokens.OpenBracket) != "" {
		ins.tkns.Remove()
	} else {
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

	ins.data, err = sqtables.NewDataSet(profile, sqtables.NewTableListFromTableDef(profile, tab), sqtables.NewColListNames(colNames))
	if err != nil {
		return err
	}

	//Values section
	err = ins.getInsertValues()
	if err != nil {
		return err
	}

	if ins.tkns.Len() != 0 {
		return sqerr.NewSyntaxf("Unexpected tokens after the values section: %s", ins.tkns.ToString())
	}
	return nil

}
func (ins *InsertStmt) getInsertValues() error {

	var vals []sqtypes.Value
	var err error

	if ins.tkns.Test(tokens.Values) != "" {
		ins.tkns.Remove()
	} else {
		return sqerr.NewSyntax("Expecting keyword VALUES")
	}
	if ins.tkns.Test(tokens.OpenBracket) == "" {
		return sqerr.NewSyntax("Expecting ( after keyword VALUES")
	}

	for {
		vals, err = ins.getValuesRow()
		if err != nil {
			return err
		}
		ins.data.Vals = append(ins.data.Vals, vals)

		if ins.tkns.Test(tokens.Comma) != "" {
			ins.tkns.Remove()
		} else {
			break
		}
	}
	return nil
}
func (ins *InsertStmt) getValuesRow() ([]sqtypes.Value, error) {
	var vals []sqtypes.Value
	vals = make([]sqtypes.Value, ins.data.NumCols())

	if ins.tkns.Test(tokens.OpenBracket) != "" {
		ins.tkns.Remove()
	} else {
		return nil, sqerr.NewSyntax("Expecting ( to start next row of VALUES")
	}
	eList, err := GetExprList(ins.tkns, tokens.CloseBracket, true)
	if err != nil {
		return nil, err
	}
	if ins.tkns.Test(tokens.CloseBracket) != "" {
		ins.tkns.Remove()
	} else {
		return nil, sqerr.NewSyntax("Expecting ) to finish row of VALUES")
	}

	vals, err = eList.GetValues()
	return vals, err
}

func (ins *InsertStmt) insertIntoTables(profile *sqprofile.SQProfile) (int, error) {
	// make sure there is a valid table
	tab, err := sqtables.GetTable(profile, ins.tableName)
	if err != nil {
		return 0, err
	}
	if tab == nil {
		return 0, sqerr.New("Table " + ins.tableName + " does not exist")
	}

	nRows, err := tab.AddRows(profile, ins.data)
	if err != nil {
		return 0, err
	}
	err = redo.Send(redo.NewInsertRows(ins.tableName, ins.data.GetColNames(), ins.data.Vals, ins.data.Ptrs))
	return nRows, err
}
