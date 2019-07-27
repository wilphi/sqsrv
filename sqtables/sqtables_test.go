package sqtables_test

import (
	"fmt"
	"log"
	"os"
	"sort"
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

type RowDataTest struct {
	TName   string
	Tab     *sqtables.TableDef
	Cols    *sqtables.ExprList
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

		if len(r.ExpRows) != data.Len() {
			t.Errorf("The number of rows returned (%d) does not match expected rows (%d)", data.Len(), len(r.ExpRows))
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
	tableName, err := cmd.CreateTableFromTokens(profile, tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	testT := sqtables.GetTable(profile, tableName)
	cols := sqtables.ColsToExpr(testT.GetCols(profile))
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

type RowPtrsTest struct {
	TestName string
	Tab      *sqtables.TableDef
	Cond     sqtables.Condition
	ExpErr   string
	ExpRows  []int64
	Sort     bool
}

func testGetRowPtrsFunc(profile *sqprofile.SQProfile, d *RowPtrsTest) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		ptrs, err := d.Tab.GetRowPtrs(profile, d.Cond, d.Sort)
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if d.ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", d.ExpErr, err.Error())
				return
			}
		}
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}

		if len(d.ExpRows) != len(ptrs) {
			t.Errorf("The number of rows returned (%d) does not match expected rows (%d)", len(ptrs), len(d.ExpRows))
			return
		}

		// make sure the row numbers match
		sort.Slice(d.ExpRows, func(i, j int) bool { return ptrs[i] < ptrs[j] })
		for i := range ptrs {
			if ptrs[i] != d.ExpRows[i] {
				t.Errorf("Returned Row num (%d) does not match expected (%d)", ptrs[i], d.ExpRows[i])
			}
		}
	}
}

func TestGetRowPtrs(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Data Setup
	stmt := "CREATE TABLE rowptrstest (rowid int, firstname string, active bool)"
	tkList := tokens.Tokenize(stmt)
	tableName, err := cmd.CreateTableFromTokens(profile, tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	testT := sqtables.GetTable(profile, tableName)
	stmt = "INSERT INTO " + tableName + "(rowid, firstname, active) VALUES " +
		"(1, \"Tim\", true), " +
		"(2, \"Ted\", true), " +
		"(3, \"Tex\", true), " +
		"(4, \"Tad\", true), " +
		"(5, \"Tom\", true), " +
		"(6, \"Top\", false)"
	tkList = tokens.Tokenize(stmt)
	_, _, err = cmd.InsertInto(profile, tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	col1Def := *testT.FindColDef(profile, "rowid")
	col2Def := *testT.FindColDef(profile, "active")
	condAll := sqtables.NewCVCond(col1Def, "<", sqtypes.NewSQInt(50))
	condNoRow := sqtables.NewCVCond(col1Def, "=", sqtypes.NewSQInt(26))
	condFirst := sqtables.NewCVCond(col1Def, "=", sqtypes.NewSQInt(1))
	condLast := sqtables.NewCVCond(col2Def, "=", sqtypes.NewSQBool(false))
	condHalf2 := sqtables.NewCVCond(col1Def, "=", sqtypes.NewSQInt(3))
	condHalf := sqtables.NewORCondition(condFirst, sqtables.NewORCondition(condHalf2, condLast))
	condMis := sqtables.NewCVCond(col1Def, "=", sqtypes.NewSQString("TEST"))
	data := []RowPtrsTest{
		{TestName: "All Rows no Cond", Tab: testT, Cond: nil, ExpErr: "", ExpRows: []int64{1, 2, 3, 4, 5, 6}, Sort: true},
		{TestName: "All Rows with Cond", Tab: testT, Cond: condAll, ExpErr: "", ExpRows: []int64{1, 2, 3, 4, 5, 6}, Sort: true},
		{TestName: "No Rows", Tab: testT, Cond: condNoRow, ExpErr: "", ExpRows: []int64{}, Sort: true},
		{TestName: "First Row", Tab: testT, Cond: condFirst, ExpErr: "", ExpRows: []int64{1}, Sort: true},
		{TestName: "Last Row", Tab: testT, Cond: condLast, ExpErr: "", ExpRows: []int64{6}, Sort: true},
		{TestName: "Half the Rows", Tab: testT, Cond: condHalf, ExpErr: "", ExpRows: []int64{1, 3, 6}, Sort: true},
		{TestName: "Condition type mismatch", Tab: testT, Cond: condMis, ExpErr: "Error: Type Mismatch in Where clause expression: rowid(INT) = TEST(STRING)", ExpRows: []int64{}, Sort: true},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetRowPtrsFunc(profile, &row))

	}
}
