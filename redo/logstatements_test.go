package redo_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

type CreateData struct {
	TestName  string
	Function  string
	TableName string
	ID        uint64
	identstr  string
	Cols      []sqtables.ColDef
}

func TestCreate(t *testing.T) {

	data := []CreateData{
		{
			TestName:  "Create new CreateDDL",
			Function:  "NEW",
			TableName: "testcreate",
			Cols: []sqtables.ColDef{
				sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
				sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false},
			},
		},
		{
			TestName:  "Set/Get ID",
			Function:  "ID",
			TableName: "testcreate",
			Cols: []sqtables.ColDef{
				sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
				sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false},
			},
			ID: 123,
		},
		{
			TestName:  "Verify Identify String",
			Function:  "IDSTR",
			TableName: "testcreate",
			Cols: []sqtables.ColDef{
				sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
				sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false},
			},
			ID: 123,
		},
		{
			TestName:  "Encode/Decode",
			Function:  "CODEC",
			TableName: "testcreate",
			Cols: []sqtables.ColDef{
				sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
				sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false},
			},
			ID: 123,
		},
		{
			TestName:  "Create Decodestatement test",
			Function:  "DECODESTATEMENT",
			TableName: "testcreate",
			Cols: []sqtables.ColDef{
				sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
				sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false},
			},
			ID: 123,
		},
		{
			TestName:  "Recreate table from redo",
			Function:  "REDO",
			TableName: "RedoCreate",
			Cols: []sqtables.ColDef{
				sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
				sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false},
			},
			ID: 123,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testCreateFunc(row))

	}
}

func testCreateFunc(d CreateData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		s := redo.NewCreateDDL(d.TableName, d.Cols)
		switch d.Function {
		case "NEW":
		case "ID":
			s.SetID(d.ID)
		case "IDSTR":
			s.SetID(d.ID)
			idstr := fmt.Sprintf("#%d - CREATE TABLE %s", d.ID, d.TableName)
			if idstr != s.Identify() {
				t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), idstr)
			}
		case "CODEC":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := &redo.CreateDDL{}
			res.Decode(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Encoding and then Decoding does not match values")
			}
		case "DECODESTATEMENT":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := redo.DecodeStatement(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Decoded Statement does not match initial values")
			}
		case "REDO":
			s.SetID(d.ID)
			profile := sqprofile.CreateSQProfile()

			err := s.Recreate(profile)
			if err != nil {
				t.Errorf("Error recreating LogStatement: %s", err)
			}
			tab := sqtables.GetTable(profile, d.TableName)
			if tab == nil {
				t.Errorf("Table %s has not been recreated", d.TableName)
			}
			cl := tab.GetCols(profile)
			cd := cl.GetColDefs()
			if !reflect.DeepEqual(d.Cols, cd) {
				t.Errorf("Columns defintions are not the same for Recreated table")
			}
		}

		if (s.TableName != d.TableName) || !reflect.DeepEqual(s.Cols, d.Cols) {
			t.Error("Columns do not match expected")
		}

		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
		}

	}
}

type InsertData struct {
	TestName  string
	Function  string
	TableName string
	Cols      []string
	Data      [][]sqtypes.Value
	RowPtrs   []int64
	ID        uint64
	Identstr  string
	ExpErr    string
}

func TestInsert(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tab := sqtables.CreateTableDef("testInsertRedo",
		sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
		sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false})
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Errorf("Error setting up table for TestInsert: %s", err)
	}
	data := []InsertData{
		{
			TestName:  "Insert Row",
			Function:  "NEW",
			TableName: "testInsertRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs: []int64{0, 1, 2},
			ID:      123,
		},
		{
			TestName:  "Insert ID String",
			Function:  "IDSTR",
			TableName: "testInsertRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Insert Encode/Decode",
			Function:  "CODEC",
			TableName: "testInsertRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Insert DecodeStatement",
			Function:  "DECODESTATEMENT",
			TableName: "testInsertRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Insert Recreate",
			Function:  "REDO",
			TableName: "testInsertRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Insert Recreate Invalid table",
			Function:  "REDO",
			TableName: "testInsertRedo2",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
			ExpErr:   "Error: Table testInsertRedo2 does not exist",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testInsertFunc(row))

	}
}

func testInsertFunc(d InsertData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()

		s := redo.NewInsertRows(d.TableName, d.Cols, d.Data, d.RowPtrs)
		s.SetID(d.ID)
		switch d.Function {
		case "NEW":
		case "ID":
			s.SetID(d.ID)
		case "IDSTR":
			s.SetID(d.ID)
			if d.Identstr != s.Identify() {
				t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), d.Identstr)
			}
		case "CODEC":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := &redo.InsertRows{}
			res.Decode(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Encoding and then Decoding does not match values")
			}
		case "DECODESTATEMENT":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := redo.DecodeStatement(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Decoded Statement does not match initial values")
			}
		case "REDO":
			s.SetID(d.ID)
			initRowCount := 0
			profile := sqprofile.CreateSQProfile()
			tab := sqtables.GetTable(profile, d.TableName)
			if tab != nil {
				initRowCount = tab.RowCount(profile)
			}
			err := s.Recreate(profile)
			if err != nil {
				if err.Error() != d.ExpErr {
					t.Errorf("Error recreating LogStatement: %s", err)
				}
				return
			}
			if tab.RowCount(profile) != initRowCount+len(d.Data) {
				t.Errorf("RowCount is off for recreate of InsertRows")
			}
		}

		if (s.TableName != d.TableName) || !reflect.DeepEqual(s.Cols, d.Cols) {
			t.Error("Columns do not match expected")
		}

		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
		}

	}
}

type UpdateData struct {
	TestName  string
	Function  string
	TableName string
	Cols      []string
	Data      [][]sqtypes.Value
	RowPtrs   []int64
	ID        uint64
	Identstr  string
	ExpErr    string
}

func TestUpdate(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tab := sqtables.CreateTableDef("testUpdateRedo",
		sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
		sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false})
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Errorf("Error setting up table for TestInsert: %s", err)
	}
	data := []UpdateData{
		{
			TestName:  "Update Row",
			Function:  "NEW",
			TableName: "testUpdateRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs: []int64{0, 1, 2},
			ID:      123,
		},
		{
			TestName:  "Update ID String",
			Function:  "IDSTR",
			TableName: "testInsertRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - UPDATE  testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Update Encode/Decode",
			Function:  "CODEC",
			TableName: "testUpdateRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Update DecodeStatement",
			Function:  "DECODESTATEMENT",
			TableName: "testUpdateRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Update Recreate",
			Function:  "REDO",
			TableName: "testUpdateRedo",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
			ExpErr:   "Internal Error: UpdateRows.Recreate is not implemented",
		},
		{
			TestName:  "Update Recreate Invalid table",
			Function:  "REDO",
			TableName: "testUpdateRedo2",
			Cols:      []string{"col1", "col2"},
			Data: [][]sqtypes.Value{
				{sqtypes.NewSQInt(1), sqtypes.NewSQString("Row 1")},
				{sqtypes.NewSQInt(2), sqtypes.NewSQString("Row 2")},
				{sqtypes.NewSQInt(3), sqtypes.NewSQString("Row 3")},
			},
			RowPtrs:  []int64{0, 1, 2},
			ID:       123,
			Identstr: "#123 - INSERT INTO testInsertRedo : Rows = 3",
			ExpErr:   "Internal Error: UpdateRows.Recreate is not implemented",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testUpdateFunc(row))

	}
}

func testUpdateFunc(d UpdateData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()

		s := redo.NewUpdateRows(d.TableName, d.Cols, d.Data, d.RowPtrs)
		s.SetID(d.ID)
		switch d.Function {
		case "NEW":
		case "ID":
			s.SetID(d.ID)
		case "IDSTR":
			s.SetID(d.ID)
			if d.Identstr != s.Identify() {
				t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), d.Identstr)
			}
		case "CODEC":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := &redo.UpdateRows{}
			res.Decode(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Encoding and then Decoding does not match values")
			}
		case "DECODESTATEMENT":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := redo.DecodeStatement(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Decoded Statement does not match initial values")
			}
		case "REDO":
			s.SetID(d.ID)
			initRowCount := 0
			profile := sqprofile.CreateSQProfile()
			tab := sqtables.GetTable(profile, d.TableName)
			if tab != nil {
				initRowCount = tab.RowCount(profile)
			}
			err := s.Recreate(profile)
			if err != nil {
				if err.Error() != d.ExpErr {
					t.Errorf("Error recreating LogStatement: %s", err)
				}
				return
			}
			if tab.RowCount(profile) != initRowCount+len(d.Data) {
				t.Errorf("RowCount is off for recreate of UpdateRows")
			}
		}

		if (s.TableName != d.TableName) || !reflect.DeepEqual(s.Cols, d.Cols) {
			t.Error("Columns do not match expected")
		}

		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
		}

	}
}

type DeleteData struct {
	TestName  string
	Function  string
	TableName string
	RowPtrs   []int64
	ID        uint64
	Identstr  string
	ExpErr    string
}

func TestDelete(t *testing.T) {
	// Testing Data Setup
	profile := sqprofile.CreateSQProfile()
	cols := []sqtables.ColDef{
		sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
		sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false},
	}
	tab := sqtables.CreateTableDef("testDeleteRedo", cols...)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Errorf("Error setting up table for TestDelete: %s", err)
	}
	cl := sqtables.NewColListDefs(cols)
	ds := sqtables.NewDataSet(tab, cl)
	numVals := 10
	ds.Vals = make([][]sqtypes.Value, numVals)
	for i := 0; i < numVals; i++ {
		ds.Vals[i] = make([]sqtypes.Value, 2)
		ds.Vals[i][0] = sqtypes.NewSQInt(i + 1)
		ds.Vals[i][1] = sqtypes.NewSQString(fmt.Sprintf("Delete Test %d", i+1))
	}
	_, err = tab.AddRows(profile, &ds)
	if err != nil {
		t.Errorf("Error setting up table for TestDelete: %s", err)
	}

	// Test Cases
	data := []DeleteData{
		{
			TestName:  "Delete Row",
			Function:  "NEW",
			TableName: "testDeleteRedo",
			RowPtrs:   []int64{1, 5, 10},
			ID:        123,
		},
		{
			TestName:  "Delete ID String",
			Function:  "IDSTR",
			TableName: "testDeleteRedo",
			RowPtrs:   []int64{1, 5, 10},
			ID:        123,
			Identstr:  "#123 - DELETE FROM  testDeleteRedo : Rows = 3",
		},
		{
			TestName:  "Delete Encode/Decode",
			Function:  "CODEC",
			TableName: "testDeleteRedo",
			RowPtrs:   []int64{1, 5, 10},
			ID:        123,
			Identstr:  "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Delete DecodeStatement",
			Function:  "DECODESTATEMENT",
			TableName: "testDeleteRedo",
			RowPtrs:   []int64{1, 5, 10},
			ID:        123,
			Identstr:  "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Delete Recreate",
			Function:  "REDO",
			TableName: "testDeleteRedo",
			RowPtrs:   []int64{1, 5, 10},
			ID:        123,
			Identstr:  "#123 - INSERT INTO testInsertRedo : Rows = 3",
		},
		{
			TestName:  "Delete Recreate Invalid table",
			Function:  "REDO",
			TableName: "testUpdateRedo2",
			RowPtrs:   []int64{1, 5, 10},
			ID:        123,
			Identstr:  "#123 - INSERT INTO testInsertRedo : Rows = 3",
			ExpErr:    "Error: Table testUpdateRedo2 does not exist",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDeleteFunc(row))

	}
}

func testDeleteFunc(d DeleteData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()

		s := redo.NewDeleteRows(d.TableName, d.RowPtrs)
		s.SetID(d.ID)
		switch d.Function {
		case "NEW":
		case "ID":
			s.SetID(d.ID)
		case "IDSTR":
			s.SetID(d.ID)
			if d.Identstr != s.Identify() {
				t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), d.Identstr)
			}
		case "CODEC":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := &redo.DeleteRows{}
			res.Decode(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Encoding and then Decoding does not match values")
			}
		case "DECODESTATEMENT":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := redo.DecodeStatement(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Decoded Statement does not match initial values")
			}
		case "REDO":
			s.SetID(d.ID)
			initRowCount := 0
			profile := sqprofile.CreateSQProfile()
			tab := sqtables.GetTable(profile, d.TableName)
			if tab != nil {
				initRowCount = tab.RowCount(profile)
			}
			err := s.Recreate(profile)
			if err != nil {
				if err.Error() != d.ExpErr {
					t.Errorf("Error recreating LogStatement: %s", err)
				}
				return
			}

			if tab.RowCount(profile) != initRowCount-len(d.RowPtrs) {
				t.Errorf("RowCount is off for recreate of DeleteRows")
			}
		}

		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
		}

	}
}

type DropTableData struct {
	TestName  string
	Function  string
	TableName string
	ID        uint64
	identstr  string
}

func TestDropTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	cols := []sqtables.ColDef{
		sqtables.ColDef{ColName: "col1", ColType: tokens.TypeInt, Idx: 1, IsNotNull: false},
		sqtables.ColDef{ColName: "col2", ColType: tokens.TypeString, Idx: 2, IsNotNull: false},
	}
	s := redo.NewCreateDDL("testredodrop", cols)
	if s.Recreate(profile) != nil {
		t.Error("Error in data setup for TestDropTable")
		return
	}
	data := []DropTableData{
		{
			TestName:  "Create new DropDDL",
			Function:  "NEW",
			TableName: "testredodrop",
		},
		{
			TestName:  "Set/Get ID",
			Function:  "ID",
			TableName: "testredodrop",
			ID:        123,
		},
		{
			TestName:  "Verify Identify String",
			Function:  "IDSTR",
			TableName: "testredodrop",
			ID:        123,
		},
		{
			TestName:  "Encode/Decode",
			Function:  "CODEC",
			TableName: "testredodrop",
			ID:        123,
		},
		{
			TestName:  "Drop Table DecodeStatement",
			Function:  "DECODESTATEMENT",
			TableName: "testredodrop",
			ID:        123,
		},
		{
			TestName:  "Recreate DROP TABLE from redo",
			Function:  "REDO",
			TableName: "testredodrop",
			ID:        123,
		}}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDropTableFunc(row))

	}
}

func testDropTableFunc(d DropTableData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		s := redo.NewDropDDL(d.TableName)
		switch d.Function {
		case "NEW":
		case "ID":
			s.SetID(d.ID)
		case "IDSTR":
			s.SetID(d.ID)
			idstr := fmt.Sprintf("#%d - DROP TABLE %s", d.ID, d.TableName)
			if idstr != s.Identify() {
				t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), idstr)
			}
		case "CODEC":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := &redo.DropDDL{}
			res.Decode(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Encoding and then Decoding does not match values")
			}
		case "DECODESTATEMENT":
			s.SetID(d.ID)

			cdr := s.Encode()
			res := redo.DecodeStatement(cdr)
			if !reflect.DeepEqual(s, res) {
				t.Error("Decoded Statement does not match initial values")
			}
		case "REDO":
			s.SetID(d.ID)
			profile := sqprofile.CreateSQProfile()
			originalList := sqtables.ListTables(profile)
			err := s.Recreate(profile)
			if err != nil {
				t.Errorf("Error recreating LogStatement: %s", err)
			}
			tab := sqtables.GetTable(profile, d.TableName)
			if tab != nil {
				t.Errorf("Table %s has not been Dropped", d.TableName)
			}
			afterList := sqtables.ListTables(profile)
			afterList = append(afterList, d.TableName)
			sort.Strings(afterList)

			if !reflect.DeepEqual(originalList, afterList) {
				t.Errorf("TableList after recreating DROP TABLE is not correct")
			}
		}

		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
		}

	}
}
