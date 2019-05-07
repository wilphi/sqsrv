package cmd_test

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	sqt "github.com/wilphi/sqsrv/sqtables"
	tk "github.com/wilphi/sqsrv/tokens"
)

func testInsertIntoFunc(profile *sqprofile.SQProfile, tkns *tk.TokenList, errTxt string) func(*testing.T) {
	return func(t *testing.T) {
		_, err := cmd.InsertIntoOld(profile, tkns)
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
		if err == nil && errTxt != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", errTxt))
			return
		}
	}
}
func TestInsertInto(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	//make sure table exists for testing
	tab := sqt.CreateTableDef("instest",
		sqt.CreateColDef("col1", tk.TypeInt, false),
		sqt.CreateColDef("col2", tk.TypeString, false),
		sqt.CreateColDef("col3", tk.TypeBool, false))
	_, err := sqt.CreateTable(profile, tab)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	var testStruct = []struct {
		TestName    string
		Command     string
		ExpectedErr string
	}{
		{"Missing Insert", "FROM", "Error: Expecting INSERT INTO to start the statement"},
		{"INSERT ONLY", "INSERT", "Error: Expecting INSERT INTO to start the statement"},
		{"INSERT Missing tableName", "INSERT INTO", "Syntax Error: Expecting name of table for insert"},
		{"INSERT missing (", "INSERT INTO instest", "Syntax Error: Expecting ( after name of table"},
		{"INSERT missing column", "INSERT INTO instest (", "Syntax Error: Expecting name of column"},
		{"INSERT missing comma after col", "INSERT INTO instest (col1", "Syntax Error: Comma is required to separate column definitions"},
		{"INSERT missing second column", "INSERT INTO instest (col1,", "Syntax Error: Expecting name of column"},
		{"INSERT missing VALUES", "INSERT INTO instest (col1,col2,col3)", "Syntax Error: Expecting keyword VALUES"},
		{"INSERT missing ( after values", "INSERT INTO instest (col1,col2,col3) VALUES", "Syntax Error: Expecting ( after keyword VALUES"},
		{"INSERT missing value for col1", "INSERT INTO instest (col1,col2,col3) VALUES (", "Syntax Error: Expecting a value for column col1"},
		{"INSERT missing comma after first value", "INSERT INTO instest (col1,col2,col3) VALUES (123", "Syntax Error: Comma is required to separate values"},
		{"INSERT missing value for col2", "INSERT INTO instest (col1,col2,col3) VALUES (123, ", "Syntax Error: Expecting a value for column col2"},
		{"INSERT missing value for col3", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", ", "Syntax Error: Expecting a value for column col3"},
		{"INSERT missing final )", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true", "Syntax Error: Comma is required to separate values"},
		{"INSERT invalid after values section", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true) (", "Syntax Error: Unexpected tokens after the values section: ("},
		{"INSERT missing ( for start of next value", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true), test", "Syntax Error: Expecting ( to start next row of VALUES"},
		{"INSERT three values", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true)", ""},
		{"Extra comma in Column list", "INSERT INTO instest (col1,col2,col3,) VALUES (123, \"With Cols Test\", true)", "Syntax Error: Unexpected \",\" before \")\""},
		{"No Cols in Column list", "INSERT INTO instest () VALUES (123, \"With Cols Test\", true)", "Syntax Error: No columns defined for table"},
		{"Extra comma in value list", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true,)", "Syntax Error: Unexpected \",\" before \")\""},
		{"No Vals in Value list", "INSERT INTO instest (col1,col2,col3) VALUES ()", "Syntax Error: No values defined for insert"},
		{"Cols do not match Values", "INSERT INTO instest (col1,col2) VALUES (123, \"With Cols Test\", true)", "Syntax Error: The number of values (3) must match the number of columns (2)"},
		{"Values do not match Cols", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\")", "Syntax Error: The number of values (2) must match the number of columns (3)"},
		{"Value Type does not match Col Type", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", 1234)", ""},
		{"Insert target table does not exist", "INSERT INTO NotATable (col1,col2,col3) VALUES (123, \"With Cols Test\", true)", "Error: Table NotATable does not exist"},
		{"More Cols than in table", "INSERT INTO instest (col1,col2,col3, colx) VALUES (123, \"With Cols Test\", true, \"Col does not exist\")", "Error: More columns are being set than exist in table definition"},
		{"Col does not exist in table", "INSERT INTO instest (col1,col2, colx) VALUES (123, \"With Cols Test\", \"Col does not exist\")", "Error: Column (colx) does not exist in table (instest)"},
		{"Integer too large - tests invalid converion", "INSERT INTO instest (col1,col2,col3) VALUES (999999999999999999999, \"With Cols Test\", true)", "Syntax Error: \"999999999999999999999\" is not a number"},
		{"Muli row insert (3)", "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true), (456, \"Second Value Test\", true), (789, \"Third Value Test\",false)", ""},
		{"Count in Insert", "INSERT INTO instest (col1, col2, count()) values (123, \"test count\", true)", "Syntax Error: Expecting name of column"},
		{"Null in Insert", "INSERT INTO instest (col1, col2, col3) values (123, null, true)", ""},
	}
	for i, row := range testStruct {
		tlist := tk.Tokenize(row.Command)
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testInsertIntoFunc(profile, tlist, row.ExpectedErr))

	}

}

var ct sync.Once

var initTable = func() {
	tab := "CREATE TABLE insbench (id:int, col1:int, col2:string, col3:bool)"
	tlist := tk.Tokenize(tab)
	_, err := cmd.CreateTableFromTokens(sqprofile.CreateSQProfile(), *tlist)
	if err != nil {
		fmt.Printf("Unexpected Error setting up test: %s", err.Error())
	}

}
