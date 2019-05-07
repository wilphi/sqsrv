package sqtables_test

import (
	"log"
	"os"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"

	"github.com/wilphi/sqsrv/sqtables"
)

const (
	withErr    = true
	withoutErr = false
)

func TestMain(m *testing.M) {
	// setup logging
	logFile, err := os.OpenFile("sqtables_test.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)

	os.Exit(m.Run())

}

func TestCreateTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	// Test create table
	tab := sqtables.CreateTableDef("_test1", []sqtables.ColDef{sqtables.CreateColDef("col1", tokens.TypeInt, false), sqtables.CreateColDef("col2", tokens.TypeBool, false)}...)
	t.Run("CREATE TABLE _test1", testCreateTableFunc(profile, tab, withErr))

	tab = sqtables.CreateTableDef("test1", []sqtables.ColDef{sqtables.CreateColDef("col1", tokens.TypeInt, false), sqtables.CreateColDef("col2", tokens.TypeBool, false)}...)
	t.Run("CREATE TABLE test1", testCreateTableFunc(profile, tab, withoutErr))

	t.Run("CREATE TABLE test1", testCreateTableFunc(profile, tab, withErr))

	tab = sqtables.CreateTableDef("test2", []sqtables.ColDef{}...)
	t.Run("CREATE TABLE test2 (no col)", testCreateTableFunc(profile, tab, withErr))
	/*
		tab = sqtables.CreateTableDef("test2", []sqtables.ColDef{sqtables.CreateColDef("city", tokens.TypeString),
			sqtables.CreateColDef("street", tokens.TypeString),
			sqtables.CreateColDef("streetno", tokens.TypeInt)}...)
		tab.TableCols[0].ColName = "NoNameCol"
		t.Run("CREATE TABLE test2 - (no _rownum col) ", testCreateTableFunc(*tab, withErr))
	*/
	tab = sqtables.CreateTableDef("test2", []sqtables.ColDef{sqtables.CreateColDef("city", tokens.TypeString, false), sqtables.CreateColDef("street", tokens.TypeString, false), sqtables.CreateColDef("streetno", tokens.TypeInt, false)}...)
	t.Run("CREATE TABLE test2", testCreateTableFunc(profile, tab, withoutErr))

	tab = sqtables.CreateTableDef("test3", []sqtables.ColDef{sqtables.CreateColDef("city", tokens.TypeString, false), sqtables.CreateColDef("street", tokens.TypeString, false), sqtables.CreateColDef("streetno", tokens.TypeInt, false)}...)
	t.Run("CREATE TABLE test3", testCreateTableFunc(profile, tab, withoutErr))

	name := "_tables"
	t.Run("Drop Table "+name, testDropTableFunc(profile, name, withErr))

	name = "test29"
	t.Run("Drop Table "+name, testDropTableFunc(profile, name, withErr))

	name = "test2"
	t.Run("Drop Table "+name, testDropTableFunc(profile, name, withoutErr))

}

func testCreateTableFunc(profile *sqprofile.SQProfile, tab *sqtables.TableDef, expErr bool) func(*testing.T) {
	return func(t *testing.T) {
		_, err := sqtables.CreateTable(profile, tab)
		if err == nil {
			if expErr {
				t.Error("Expected error but instead success")
			}
		} else {
			log.Println(err.Error())
			if !expErr {
				t.Errorf("Unexpected Error in test: %s", err.Error())
			}
		}
	}
}

func testDropTableFunc(profile *sqprofile.SQProfile, name string, expErr bool) func(*testing.T) {
	return func(t *testing.T) {
		_, err := sqtables.DropTable(profile, name)
		if err == nil {
			if expErr {
				t.Error("Expected error but instead success")
			}
		} else {
			log.Println(err.Error())
			if !expErr {
				t.Errorf("Unexpected Error in test: %s", err.Error())
			}
		}
	}
}

type RowDataTest struct {
	TName   string
	Tab     *sqtables.TableDef
	Cols    sqtables.ColList
	Cond    sqtables.Condition
	ExpErr  string
	ExpRows []int
}

func testGetRowDataFunc(profile *sqprofile.SQProfile, r *RowDataTest) func(*testing.T) {
	return func(t *testing.T) {
		data, err := r.Tab.GetRowData(profile, r.Cols, r.Cond)
		if err != nil {
			log.Println(err.Error())
			if r.ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if r.ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", r.ExpErr, err.Error())
				return
			}
		}
		if err == nil && r.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", r.ExpErr)
			return
		}

		if len(r.ExpRows) != data.NumRows() {
			t.Errorf("The number of rows returned (%d) does not match expected rows (%d)", data.NumRows(), len(r.ExpRows))
			return
		}

		// make sure the row numbers match
		for i := range data.Vals {
			if !data.Vals[i][0].Equal(sqtypes.NewSQInt(r.ExpRows[i])) {
				t.Errorf("Returned Row num (%d) does not match expected (%d)", data.Vals[i][0], r.ExpRows[i])
			}
		}
	}
}

func TestGetRowData(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Data Setup
	//colNames := []string{"rownum", "col1", "col2", "col3", "col4"}
	stmt := "CREATE TABLE rowdatatest (rownum int, col1 int, col2 string, col3 int, col4 bool)"
	tkList := tokens.Tokenize(stmt)
	tableName, err := cmd.CreateTableFromTokens(profile, *tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	testT := sqtables.GetTable(profile, tableName)
	cols := testT.GetCols(profile)
	stmt = "INSERT INTO " + tableName + "(rownum, col1, col2, col3, col4) VALUES (1,5,\"d test string\", 10, true), (2,7,\"f test string\", 100, false) "
	tkList = tokens.Tokenize(stmt)
	_, _, err = cmd.InsertInto(profile, tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	col1Def := *testT.FindColDef(profile, "col1")
	condEQ5 := sqtables.NewCVCond(col1Def, "=", sqtypes.NewSQInt(5))
	condEQ6 := sqtables.NewCVCond(col1Def, "=", sqtypes.NewSQInt(6))
	condLT5 := sqtables.NewCVCond(col1Def, "<", sqtypes.NewSQInt(5))
	condLT6 := sqtables.NewCVCond(col1Def, "<", sqtypes.NewSQInt(6))
	testData := []RowDataTest{
		{"col1(5) = 5 ->1", testT, cols, condEQ5, "", []int{1}},
		{"col1(6) = 5 ->0", testT, cols, condEQ6, "", []int{}},
		{"col1 < 5 ->0", testT, cols, condLT5, "", []int{}},
		{"col1 < 7 ->1", testT, cols, condLT6, "", []int{1}},
	}

	for _, rw := range testData {
		t.Run(rw.TName, testGetRowDataFunc(profile, &rw))
	}
}
