package cmd_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	sqt "github.com/wilphi/sqsrv/sqtables"
	tk "github.com/wilphi/sqsrv/tokens"
)

func testSelectFunc(profile *sqprofile.SQProfile, tkns *tk.TokenList, nExp int, cols []string, errTxt string) func(*testing.T) {
	return func(t *testing.T) {
		data, err := cmd.SelectFromTokens(profile, tkns)
		if err != nil {
			log.Println(err.Error())
			if errTxt == "" {
				t.Error(fmt.Sprintf("Unexpected Error in test: %s", err.Error()))
				return
			}
			if errTxt != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", errTxt, err.Error()))
				return
			}
			return
		}
		if data.NumRows() != nExp {
			fmt.Println("Expected: ", cols)
			fmt.Println("Result: ", data.GetColNames())
			t.Error(fmt.Sprintf("The number of rows returned (%d) does not match expected rows (%d)", data.NumRows(), nExp))
			return
		}
		if err == nil && errTxt != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", errTxt))
			return
		}
		if cols == nil && data.GetColNames() != nil {
			t.Error(fmt.Sprintf("Expecting nil columns but got %d of them", data.NumCols()))
			return
		}
		if data.NumCols() != len(cols) {
			fmt.Println("Expected: ", cols)
			fmt.Println("Result: ", data.GetColNames())
			t.Error(fmt.Sprintf("Number of columns returned (%d) does not match expected number of cols(%d)", data.NumCols(), len(cols)))
			return
		}
		for i, colName := range data.GetColNames() {
			if cols[i] != colName {
				t.Error(fmt.Sprintf("Expecting col named (%s) but returned (%s) instead", cols[i], colName))
			}
		}
	}
}
func TestSelectFromTokens(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	//make sure table exists for testing
	tab := sqt.CreateTableDef("seltest",
		sqt.CreateColDef("col1", tk.TypeInt, false),
		sqt.CreateColDef("col2", tk.TypeString, false),
		sqt.CreateColDef("col3", tk.TypeBool, false))
	_, err := sqt.CreateTable(profile, tab)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	// Test to see what happens with empty table
	command := "SELECT col1, col2, col3 from seltest"

	tkList := tk.Tokenize(command)
	t.Run("Select from empty table", testSelectFunc(profile, tkList, 0, []string{"col1", "col2", "col3"}, ""))

	testData := "INSERT INTO seltest (col1, col2, col3) VALUES (123, \"With Cols Test\", true), (456, \"Seltest 2\", true), (789, \"Seltest 3\", false)"
	tkList = tk.Tokenize(testData)
	if _, err := cmd.InsertIntoOld(profile, tkList); err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	var testStruct = []struct {
		TestName string
		Command  string
		ExpErr   string
		ExpRows  int
		ExpCols  []string
	}{
		{"SELECT Where invalid", "SELECT col1 FROM seltest WHERE col1=9999999999999999999999", "Syntax Error: \"9999999999999999999999\" is not a number", 1, []string{"col1"}},
		{TestName: "SELECT only", Command: "SELECT", ExpErr: "Syntax Error: Expecting name of column", ExpRows: 0, ExpCols: []string{}},
		{"SELECT missing comma", "SELECT col1", "Syntax Error: Comma is required to separate column definitions", 0, []string{}},
		{"SELECT missing FROM", "SELECT col1, col2, col3", "Syntax Error: Comma is required to separate column definitions", 0, []string{}},
		{"SELECT missing FROM", "SELECT col1, col2, col3 FROM", "Syntax Error: Expecting table name in select statement", 0, []string{}},
		{"SELECT from seltest", "SELECT col1, col2, col3 FROM seltest", "", 3, []string{"col1", "col2", "col3"}},
		{"SELECT * from seltest", "SELECT * FROM seltest", "", 3, []string{"col1", "col2", "col3"}},
		{"Invalid table name", "SELECT col1, col2 FROM NotATable", "Error: Table NotATable does not exist for select statement", 0, []string{}},
		{"Invalid column name", "SELECT col1, col2, colx FROM seltest", "Error: Table seltest does not have a column named colx", 0, []string{}},
		{"SELECT * tableName", "SELECT * seltest", "Syntax Error: Expecting FROM", 0, []string{}},
		{"Select * from NotATable", "Select * from NotATable", "Error: Table NotATable does not exist for select statement", 0, []string{}},
		{"SELECT too many columns", "SELECT col1, col2, col3, colx FROM seltest", "Error: Table seltest does not have a column named colx", 0, []string{}},
		{"SELECT Where", "SELECT col1 FROM seltest WHERE col1=456", "", 1, []string{"col1"}},
		{"SELECT COUNT", "SELECT COUNT FROM seltest", "Syntax Error: Count must be followed by ()", 1, []string{"COUNT"}},
		{"SELECT COUNT(", "SELECT COUNT( FROM seltest", "Syntax Error: Count must be followed by ()", 1, []string{"COUNT"}},
		{"SELECT COUNT)", "SELECT COUNT) FROM seltest", "Syntax Error: Count must be followed by ()", 1, []string{"COUNT"}},
		{"SELECT COUNT()", "SELECT COUNT() FROM seltest", "", 1, []string{"COUNT"}},
	}

	for i, row := range testStruct {
		tlist := tk.Tokenize(row.Command)
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSelectFunc(profile, tlist, row.ExpRows, row.ExpCols, row.ExpErr))

	}
}
