package redo_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

type CreateData struct {
	TestName  string
	TableName string
	ID        uint64
	identstr  string
	Cols      []sqtables.ColDef
}

func TestCreate(t *testing.T) {

	data := []CreateData{
		{
			TestName:  "Recreate table from redo",
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
		// Test Get/Set Id
		s.SetID(d.ID)
		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
			return
		}

		// Test identity string
		idstr := fmt.Sprintf("#%d - CREATE TABLE %s", d.ID, d.TableName)
		if idstr != s.Identify() {
			t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), idstr)
			return
		}

		// Test Encode/Decode
		cdr := s.Encode()
		res := &redo.CreateDDL{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}

		// Make sure the function DecodeStatement can properly pick and decode the statement type
		cdr = s.Encode()
		resStmt := redo.DecodeStatement(cdr)
		if !reflect.DeepEqual(s, resStmt) {
			t.Error("Decoded Statement does not match initial values")
			return
		}

		// Test Recreate
		profile := sqprofile.CreateSQProfile()

		err := s.Recreate(profile)
		if err != nil {
			t.Errorf("Error recreating LogStatement: %s", err)
			return
		}
		tab, err := sqtables.GetTable(profile, d.TableName)
		if err != nil {
			t.Error(err)
			return
		}

		if tab == nil {
			t.Errorf("Table %s has not been recreated", d.TableName)
			return
		}

		// Clean up table when done
		defer sqtables.DropTable(profile, d.TableName)

		cl := tab.GetCols(profile)
		cd := cl.GetColDefs()
		if !reflect.DeepEqual(d.Cols, cd) {
			t.Errorf("Columns defintions are not the same for Recreated table")
			return
		}

		if (s.TableName != d.TableName) || !reflect.DeepEqual(s.Cols, d.Cols) {
			t.Error("Columns do not match expected")
			return
		}
	}
}

type InsertData struct {
	TestName  string
	TableName string
	Cols      []string
	Data      sqtypes.RawVals
	RowPtrs   sqptr.SQPtrs
	ID        uint64
	Identstr  string
	ExpErr    string
	ExpData   sqtypes.RawVals
}

func TestInsert(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	_, err := cmd.CreateTableFromTokens(profile, tokens.Tokenize("Create table testInsertRedo (col1 int, col2 string)"))
	if err != nil {
		t.Errorf("Error setting up table for TestInsert: %s", err)
	}
	data := []InsertData{
		{
			TestName:  "Insert Recreate",
			TableName: "testInsertRedo",
			Cols:      []string{"col1", "col2"},
			Data:      sqtypes.RawVals{{1, "Row 1"}, {2, "Row 2"}, {3, "Row 3"}},
			RowPtrs:   sqptr.SQPtrs{0, 1, 2},
			ID:        123,
			Identstr:  "#123 - INSERT INTO testInsertRedo : Rows = 3",
			ExpData:   sqtypes.RawVals{{1, "Row 1"}, {2, "Row 2"}, {3, "Row 3"}},
		},
		{
			TestName:  "Insert Recreate Invalid table",
			TableName: "testInsertRedo2",
			Cols:      []string{"col1", "col2"},
			Data:      sqtypes.RawVals{{1, "Row 1"}, {2, "Row 2"}, {3, "Row 3"}},
			RowPtrs:   sqptr.SQPtrs{0, 1, 2},
			ID:        123,
			Identstr:  "#123 - INSERT INTO testInsertRedo2 : Rows = 3",
			ExpErr:    "Error: Table testInsertRedo2 does not exist",
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
		var err error
		var initPtrs sqptr.SQPtrs

		data := sqtypes.CreateValuesFromRaw(d.Data)
		s := redo.NewInsertRows(d.TableName, d.Cols, data, d.RowPtrs)

		// Test Get/Set ID
		s.SetID(d.ID)
		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
			return
		}

		// Test the Identifier
		if d.Identstr != s.Identify() {
			t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), d.Identstr)
			return
		}
		// Test Encode/Decode
		cdr := s.Encode()
		res := &redo.InsertRows{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}

		// Make sure the function DecodeStatement can properly pick and decode the statement type
		cdr = s.Encode()
		resStmt := redo.DecodeStatement(cdr)
		if !reflect.DeepEqual(s, resStmt) {
			t.Error("Decoded Statement does not match initial values")
			return
		}
		// test recreate
		profile := sqprofile.CreateSQProfile()
		tab, err := sqtables.GetTable(profile, d.TableName)
		if err != nil {
			t.Error(err)
			return
		}

		if tab != nil {
			initPtrs, err = tab.GetRowPtrs(profile, nil, true)
			if err != nil {
				t.Errorf("Unable to get table data for %s", d.TableName)
				return
			}
		}
		err = s.Recreate(profile)
		if err != nil {
			if err.Error() != d.ExpErr {
				t.Errorf("Error recreating LogStatement: %s", err)
			}
			return
		}
		// Verify inserted data
		afterPtrs, err := tab.GetRowPtrs(profile, nil, true)
		if err != nil {
			t.Errorf("Unable to get table data for %s", d.TableName)
			return
		}
		ptrs := sqptr.NotIn(afterPtrs, initPtrs)

		actData, err := tab.GetRowDataFromPtrs(profile, ptrs)
		if err != nil {
			t.Errorf("Unable to get table data for %s", d.TableName)
			return
		}
		expData := sqtypes.CreateValuesFromRaw(d.ExpData)
		if !reflect.DeepEqual(expData, actData.Vals) {
			t.Error("Expected values do not match actual values")
			return
		}

		if (s.TableName != d.TableName) || !reflect.DeepEqual(s.Cols, d.Cols) {
			t.Error("Columns do not match expected")
			return
		}

	}
}

type UpdateData struct {
	TestName  string
	TableName string
	Cols      []string
	Vals      []sqtypes.Raw
	RowPtrs   sqptr.SQPtrs
	ID        uint64
	Identstr  string
	ExpErr    string
	ExpVals   sqtypes.RawVals
}

func TestUpdate(t *testing.T) {

	data := []UpdateData{
		{
			TestName:  "Update Row",
			TableName: "testUpdateRedo",
			Cols:      []string{"col1", "col2"},
			Vals:      []sqtypes.Raw{1, "Row X"},
			RowPtrs:   sqptr.SQPtrs{1, 2, 5},
			ID:        123,
			Identstr:  "#123 - UPDATE  testUpdateRedo : Rows = 3",
			ExpVals: sqtypes.RawVals{
				{1, "Row X"}, {1, "Row X"}, {3, "test row 3"},
				{4, "test row 4"}, {1, "Row X"}, {6, "test row 6"},
			},
		},
		{
			TestName:  "Update Recreate Invalid table",
			TableName: "testUpdateRedo2",
			Cols:      []string{"col1", "col2"},
			Vals:      []sqtypes.Raw{1, "Row X"},
			RowPtrs:   sqptr.SQPtrs{1, 2, 5},
			ID:        123,
			Identstr:  "#123 - UPDATE  testUpdateRedo2 : Rows = 3",
			ExpErr:    "Error: Table testUpdateRedo2 does not exist",
		},
		{
			TestName:  "Update Recreate Null values",
			TableName: "testUpdateRedo",
			Cols:      []string{"col1", "col2"},
			Vals:      []sqtypes.Raw{nil, "Row X"},
			RowPtrs:   sqptr.SQPtrs{1, 2, 5},
			ID:        123,
			Identstr:  "#123 - UPDATE  testUpdateRedo : Rows = 3",
			ExpErr:    "Error: Column \"col1\" in Table \"testupdateredo\" can not be NULL",
		},
		{
			TestName:  "Update Recreate Extra Column",
			TableName: "testUpdateRedo",
			Cols:      []string{"col1", "col2", "col3"},
			Vals:      []sqtypes.Raw{10, "Row X", nil},
			RowPtrs:   sqptr.SQPtrs{1, 2, 5},
			ID:        123,
			Identstr:  "#123 - UPDATE  testUpdateRedo : Rows = 3",
			ExpErr:    "Error: Column (col3) does not exist in table (testupdateredo)",
		},
		{
			TestName:  "Update Recreate Row does not Exist",
			TableName: "testUpdateRedo",
			Cols:      []string{"col1", "col2", "col3"},
			Vals:      []sqtypes.Raw{10, "Row X", nil},
			RowPtrs:   sqptr.SQPtrs{999, 2, 5},
			ID:        123,
			Identstr:  "#123 - UPDATE  testUpdateRedo : Rows = 3",
			ExpErr:    "Internal Error: Row 999 does not exist for update",
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

		// Create table & data
		profile := sqprofile.CreateSQProfile()
		_, err := cmd.CreateTableFromTokens(profile, tokens.Tokenize("Create table testUpdateRedo (col1 int not null, col2 string)"))
		if err != nil {
			t.Errorf("Error setting up table for TestUpdate: %s", err)
			return
		}
		defer sqtables.DropTable(profile, "testUpdateRedo")
		ins := "INSERT INTO testUpdateRedo (col1, col2) VALUES " +
			"(1, \"test row 1\"), " +
			"(2, \"test row 2\"), " +
			"(3, \"test row 3\"), " +
			"(4, \"test row 4\"), " +
			"(5, \"test row 5\"), " +
			"(6, \"test row 6\")"
		_, _, err = cmd.InsertInto(profile, tokens.Tokenize(ins))
		if err != nil {
			t.Errorf("Error setting up table for TestUpdate: %s", err)
			return
		}
		// Make sure the create for Update works
		valArray := sqtypes.CreateValueArrayFromRaw(d.Vals)
		eList := sqtables.NewExprListFromValues(valArray)
		s := redo.NewUpdateRows(d.TableName, d.Cols, eList, d.RowPtrs)
		if (s.TableName != d.TableName) || !reflect.DeepEqual(s.Cols, d.Cols) {
			t.Error("Columns do not match expected")
		}

		// verify Get/Set for ID
		s.SetID(d.ID)
		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
		}
		// Test the identstr
		if d.Identstr != s.Identify() {
			t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), d.Identstr)
		}
		// test Encode/Decode
		cdr := s.Encode()
		res := &redo.UpdateRows{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
		}
		// Make sure the function DecodeStatement can properly pick and decode the statement type
		cdr = s.Encode()
		resStmt := redo.DecodeStatement(cdr)
		if !reflect.DeepEqual(s, resStmt) {
			t.Error("Decoded Statement does not match initial values")
		}

		// Recreate the statement
		initRowCount := 0

		tab, err := sqtables.GetTable(profile, d.TableName)
		if err != nil {
			t.Error(err)
			return
		}

		if tab != nil {
			initRowCount, err = tab.RowCount(profile)
			if err != nil {
				t.Error(err)
				return
			}

		}
		err = s.Recreate(profile)
		if err != nil {
			if err.Error() != d.ExpErr {
				t.Errorf("Error recreating LogStatement: %s", err)
			}
			// error matches expected err so return
			return
		}
		rc, err := tab.RowCount(profile)
		if err != nil {
			t.Error(err)
			return
		}
		if rc != initRowCount {
			t.Errorf("RowCount is off for recreate of UpdateRows")
			return
		}
		expData := sqtypes.CreateValuesFromRaw(d.ExpVals)
		ptrs, err := tab.GetRowPtrs(profile, nil, true)
		if err != nil {
			t.Errorf("Error fetching data before recreate: %s", err)
			return
		}
		actData, err := tab.GetRowDataFromPtrs(profile, ptrs)
		if err != nil {
			t.Errorf("Error fetching data before recreate: %s", err)
			return
		}
		if !reflect.DeepEqual(actData.Vals, expData) {
			fmt.Println("Act:", actData.Vals)
			fmt.Println("expDAta:", expData)
			t.Errorf("Update expected data does not match initial data")
			return
		}
	}
}

type DeleteData struct {
	TestName  string
	Function  string
	TableName string
	RowPtrs   sqptr.SQPtrs
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
	ds, err := sqtables.NewDataSet(profile, sqtables.NewTableListFromTableDef(profile, tab), sqtables.ColsToExpr(cl), nil)
	if err != nil {
		t.Errorf("Error setting up table for TestDelete: %s", err)
	}
	numVals := 10
	ds.Vals = make([][]sqtypes.Value, numVals)
	for i := 0; i < numVals; i++ {
		ds.Vals[i] = make([]sqtypes.Value, 2)
		ds.Vals[i][0] = sqtypes.NewSQInt(i + 1)
		ds.Vals[i][1] = sqtypes.NewSQString(fmt.Sprintf("Delete Test %d", i+1))
	}
	_, err = tab.AddRows(profile, ds)
	if err != nil {
		t.Errorf("Error setting up table for TestDelete: %s", err)
	}

	// Test Cases
	data := []DeleteData{

		{
			TestName:  "Delete Recreate",
			Function:  "REDO",
			TableName: "testDeleteRedo",
			RowPtrs:   sqptr.SQPtrs{1, 5, 10},
			ID:        123,
			Identstr:  "#123 - DELETE FROM  testDeleteRedo : Rows = 3",
		},
		{
			TestName:  "Delete Recreate Invalid table",
			Function:  "REDO",
			TableName: "testDeleteRedo2",
			RowPtrs:   sqptr.SQPtrs{1, 5, 10},
			ID:        123,
			Identstr:  "#123 - DELETE FROM  testDeleteRedo2 : Rows = 3",
			ExpErr:    "Error: Table testDeleteRedo2 does not exist",
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
		var err error
		s := redo.NewDeleteRows(d.TableName, d.RowPtrs)
		// Test Set/Get ID
		s.SetID(d.ID)
		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
			return
		}

		// Test Identify string
		if d.Identstr != s.Identify() {
			t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), d.Identstr)
			return
		}

		// Test Encode/Decode
		cdr := s.Encode()
		res := &redo.DeleteRows{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}

		// Make sure the function DecodeStatement can properly pick and decode the statement type
		cdr = s.Encode()
		resStmt := redo.DecodeStatement(cdr)
		if !reflect.DeepEqual(s, resStmt) {
			t.Error("Decoded Statement does not match initial values")
			return
		}

		// test recreate
		initRowCount := 0
		profile := sqprofile.CreateSQProfile()
		tab, err := sqtables.GetTable(profile, d.TableName)
		if err != nil {
			t.Error(err)
			return
		}

		if tab != nil {
			initRowCount, err = tab.RowCount(profile)
			if err != nil {
				t.Error(err)
				return
			}
		}
		err = s.Recreate(profile)
		if err != nil {
			if err.Error() != d.ExpErr {
				t.Errorf("Error recreating LogStatement: %s", err)
			}
			return
		}
		rc, err := tab.RowCount(profile)
		if err != nil {
			t.Error(err)
			return
		}
		if rc != initRowCount-len(d.RowPtrs) {
			t.Errorf("RowCount is off for recreate of DeleteRows")
			return
		}
	}
}

type DropTableData struct {
	TestName  string
	TableName string
	ID        uint64
	Identstr  string
	ExpErr    string
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
			TestName:  "Recreate DROP TABLE from redo",
			TableName: "testredodrop",
			ID:        123,
			Identstr:  "#123 - DROP TABLE testredodrop",
		},
		{
			TestName:  "Recreate DROP TABLE from redo invalid table",
			TableName: "testredodrop2",
			ID:        123,
			Identstr:  "#123 - DROP TABLE testredodrop2",
			ExpErr:    "Error: Invalid Name: Table testredodrop2 does not exist",
		},
	}

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

		// Test Set/Get ID
		s.SetID(d.ID)
		if s.GetID() != d.ID {
			t.Errorf("ID (%d) does not match Expected ID (%d)", s.GetID(), d.ID)
			return
		}

		// Test Identify
		if d.Identstr != s.Identify() {
			t.Errorf("Identity string (%s) does not match expected (%s)", s.Identify(), d.Identstr)
			return
		}

		// Test Encode/Decode
		cdr := s.Encode()
		res := &redo.DropDDL{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}
		// test DecodeStatment

		cdr = s.Encode()
		resStmt := redo.DecodeStatement(cdr)
		if !reflect.DeepEqual(s, resStmt) {
			t.Error("Decoded Statement does not match initial values")
			return
		}

		// Test recreate
		profile := sqprofile.CreateSQProfile()
		originalList, err := sqtables.CatalogTables(profile)
		if err != nil {
			t.Error(err)
			return
		}

		err = s.Recreate(profile)
		if err != nil {
			if err.Error() != d.ExpErr {
				t.Errorf("Error recreating LogStatement: %s", err)
			}
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}

		tab, err := sqtables.GetTable(profile, d.TableName)
		if err != nil {
			t.Error(err)
			return
		}

		if tab != nil {
			t.Errorf("Table %s has not been Dropped", d.TableName)
			return
		}
		afterList, err := sqtables.CatalogTables(profile)
		if err != nil {
			t.Error(err)
			return
		}

		afterList = append(afterList, d.TableName)
		sort.Strings(afterList)

		if !reflect.DeepEqual(originalList, afterList) {
			t.Errorf("TableList after recreating DROP TABLE is not correct")
			return
		}

	}
}

func TestDecodeErr(t *testing.T) {
	s := redo.NewDropDDL("ErrTest")
	s2 := redo.NewDeleteRows("test", sqptr.SQPtrs{1, 2, 3})

	t.Run("Create", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("%s did not panic", t.Name())
			}
		}() // Test Encode/Decode
		cdr := s.Encode()
		res := &redo.CreateDDL{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}

	})
	t.Run("Insert", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("%s did not panic", t.Name())
			}
		}() // Test Encode/Decode
		cdr := s.Encode()
		res := &redo.InsertRows{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}

	})
	t.Run("Update", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("%s did not panic", t.Name())
			}
		}() // Test Encode/Decode
		cdr := s.Encode()
		res := &redo.UpdateRows{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}
	})
	t.Run("Delete", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("%s did not panic", t.Name())
			}
		}() // Test Encode/Decode
		cdr := s.Encode()
		res := &redo.DeleteRows{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}
	})
	t.Run("Drop", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("%s did not panic", t.Name())
			}
		}() // Test Encode/Decode
		cdr := s2.Encode()
		res := &redo.DropDDL{}
		res.Decode(cdr)
		if !reflect.DeepEqual(s, res) {
			t.Error("Encoding and then Decoding does not match values")
			return
		}
	})

}
