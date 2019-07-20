package cmd

import (
	"fmt"

	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"

	log "github.com/sirupsen/logrus"
	e "github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	sqtypes "github.com/wilphi/sqsrv/sqtypes"
	t "github.com/wilphi/sqsrv/tokens"
)

// InsertStmt - structure to store decoded Insert Statement
type InsertStmt struct {
	tkns      *t.TokenList
	tableName string
	//cols      []string
	//vals      [][]sqtypes.Value
	data *sqtables.DataSet
}

/*
// InsertIntoOld -
func InsertIntoOld(profile *sqprofile.SQProfile, tl *t.TokenList) (int, error) {
	ins, err := CreateInsertStmt(tl)
	if err != nil {
		return -1, err
	}
	err = ins.Decode(profile)
	if err != nil {
		return -1, err
	}
	i, err := ins.insertIntoTables(profile)
	return i, err
}
*/

// InsertInto -
func InsertInto(profile *sqprofile.SQProfile, tl *t.TokenList) (string, *sqtables.DataSet, error) {
	ins, err := CreateInsertStmt(tl)
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

// CreateInsertStmt - Create the Insert Statement
func CreateInsertStmt(tl *t.TokenList) (*InsertStmt, error) {

	// make that this is an Insert
	if tl.Test(t.Insert) == "" {
		return nil, e.New("Expecting INSERT INTO to start the statement")
	}
	tl.Remove()
	if tl.Test(t.Into) == "" {
		return nil, e.New("Expecting INSERT INTO to start the statement")
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
	if val := ins.tkns.Test(t.Ident); val != "" {
		ins.tableName = val
		ins.tkns.Remove()
	} else {
		return e.NewSyntax("Expecting name of table for insert")
	}

	if ins.tkns.Test(t.OpenBracket) != "" {
		ins.tkns.Remove()
	} else {
		return e.NewSyntax("Expecting ( after name of table")
	}

	colNames, err = GetIdentList(ins.tkns, t.CloseBracket)
	if err != nil {
		return err
	}
	tab := sqtables.GetTable(profile, ins.tableName)
	if tab == nil {
		return sqerr.New("Table " + ins.tableName + " does not exist")
	}

	ins.data, err = sqtables.NewDataSet(profile, tab, sqtables.NewColListNames(colNames))
	if err != nil {
		return err
	}

	//Values section
	err = ins.getInsertValues()
	if err != nil {
		return err
	}

	if ins.tkns.Len() != 0 {
		return e.NewSyntax(fmt.Sprintf("Unexpected tokens after the values section: %s", ins.tkns.ToString()))
	}
	return nil

}
func (ins *InsertStmt) getInsertValues() error {

	var vals []sqtypes.Value
	var err error

	if ins.tkns.Test(t.Values) != "" {
		ins.tkns.Remove()
	} else {
		return e.NewSyntax("Expecting keyword VALUES")
	}
	if ins.tkns.Test(t.OpenBracket) == "" {
		return e.NewSyntax("Expecting ( after keyword VALUES")
	}

	for {
		vals, err = ins.getValuesRow()
		if err != nil {
			return err
		}
		ins.data.Vals = append(ins.data.Vals, vals)

		if ins.tkns.Test(t.Comma) != "" {
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

	if ins.tkns.Test(t.OpenBracket) != "" {
		ins.tkns.Remove()
	} else {
		return nil, e.NewSyntax("Expecting ( to start next row of VALUES")
	}
	eList, err := GetExprList(ins.tkns, t.CloseBracket, true)
	if err != nil {
		return nil, err
	}
	vals, err = eList.GetValues()
	return vals, err
}

func (ins *InsertStmt) insertIntoTables(profile *sqprofile.SQProfile) (int, error) {
	/*
		// make sure cols, vals, valtypes are the same len
		if len(cols) != len(vals) {
			return e.New("cols, vals are not equal length")
		}
	*/
	// make sure there is a valid table
	tab := sqtables.GetTable(profile, ins.tableName)
	if tab == nil {
		return 0, e.New("Table " + ins.tableName + " does not exist")
	}

	nRows, err := tab.AddRows(profile, ins.data)
	if err != nil {
		return 0, err
	}
	err = redo.Send(redo.NewInsertRows(ins.tableName, ins.data.GetColNames(), ins.data.Vals, ins.data.Ptrs))
	return nRows, err
}
