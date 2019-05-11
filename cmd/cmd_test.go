package cmd_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"

	"github.com/wilphi/sqsrv/cmd"
	sqt "github.com/wilphi/sqsrv/sqtables"
	tk "github.com/wilphi/sqsrv/tokens"
)

const (
	withErr    = true
	withoutErr = false
)

func TestMain(m *testing.M) {
	// setup logging
	logFile, err := os.OpenFile("cmd_test.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)

	os.Exit(m.Run())

}
func testGetIdentListFunc(tkns *tk.TokenList, terminator *tk.Token, expectedIdents []string, errTxt string) func(*testing.T) {
	return func(t *testing.T) {
		rTkns, rIdents, err := cmd.GetIdentList(tkns, terminator, false)
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
		if rTkns.Len() != 0 {
			t.Error("All tokens should be consumed by test")
			return
		}
		if len(expectedIdents) != len(rIdents) {
			t.Error(fmt.Sprintf("The length Expected Idents (%d) and returned Idents (%d) do not match", len(expectedIdents), len(rIdents)))
			return
		}
		for i, id := range expectedIdents {
			if rIdents[i] != id {
				t.Error(fmt.Sprintf("Expected Ident (%s) does not match returned Ident (%s)", id, rIdents))
			}
		}

	}
}
func TestGetIdentList(t *testing.T) {

	var testStruct = []struct {
		TestName     string
		Terminator   *tk.Token
		Command      string
		ExpectedErr  string
		ExpectedCols []string
	}{
		{"One Col", tk.SYMBOLS[')'], "col1", "Syntax Error: Comma is required to separate column definitions", nil},
		{"Expect another Col", tk.SYMBOLS[')'], "col1,", "Syntax Error: Expecting name of column", nil},
		{"Two Col", tk.SYMBOLS[')'], "col1, col2", "Syntax Error: Comma is required to separate column definitions", nil},
		{"Expect a third Col", tk.SYMBOLS[')'], "col1,col2,", "Syntax Error: Expecting name of column", nil},
		{"Three Col", tk.SYMBOLS[')'], "col1, col2, col3", "Syntax Error: Comma is required to separate column definitions", nil},
		{"Complete col definition with )", tk.SYMBOLS[')'], "col1, col2, col3)", "", []string{"col1", "col2", "col3"}},
		{"Complete col definition with FROM", tk.AllWordTokens[tk.From], "firstname, lastname, phonenum FROM", "", []string{"firstname", "lastname", "phonenum"}},
		{"Extra Comma in list", tk.SYMBOLS[')'], "col1, col2, col3,)", "Syntax Error: Unexpected \",\" before \")\"", []string{"col1", "col2", "col3"}},
		{"No Cols in list", tk.SYMBOLS[')'], ")", "Syntax Error: No columns defined for table", []string{"col1", "col2", "col3"}},
	}

	for i, row := range testStruct {
		tlist := tk.Tokenize(row.Command)
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetIdentListFunc(tlist, row.Terminator, row.ExpectedCols, row.ExpectedErr))

	}

}

func TestGetWhereConditions(t *testing.T) {

	//make sure table exists for testing
	profile := sqprofile.CreateSQProfile()

	tab := sqt.CreateTableDef("cmdtest",
		sqt.CreateColDef("col1", tk.TypeInt, false),
		sqt.CreateColDef("col2", tk.TypeString, false),
		sqt.CreateColDef("col3", tk.TypeBool, false))
	err := sqt.CreateTable(profile, tab)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	var testStruct = []struct {
		TestName     string
		Command      string
		TableD       *sqt.TableDef
		ExpectedCond string
		ExpectedErr  string
	}{
		{"Equal Condition", "col1 = 1", tab, "col1 = 1", ""},
		{"Reverse Equal Condition", "1 = col1", tab, "col1 = 1", "Syntax Error: Expecting a column name in where clause"},
		{"No Col Condition", "2=1", tab, "", "Syntax Error: Expecting a column name in where clause"},
		{"Invalid Operator", "col1 ~ 1", tab, "col1 = 1", "Syntax Error: Expecting an operator after column name (col1) in where clause"},
		{"Missing Value", "col1 = ", tab, "col1 = 1", "Syntax Error: Expecting a value in where clause after col1 ="},
		{"Invalid Value", "col1 = 9999999999999999999999 ", tab, "col1 = 1", "Syntax Error: \"9999999999999999999999\" is not a number"},
		{"Simple AND Condition", "col1 = 1 AND col2 = \"test\"", tab, "(col1 = 1 AND col2 = \"test\")", ""},
		{"Simple OR Condition", "col1 = 1 OR col2 = \"test\"", tab, "(col1 = 1 OR col2 = \"test\")", ""},
		{"Multiple AND Conditions", "col1 = 1 AND col2 = \"test\" AND col3 = false", tab, "((col1 = 1 AND col2 = \"test\") AND col3 = false)", ""},
		{"Multiple OR Conditions", "col1 = 1 OR col2 = \"test\" OR col3 = false", tab, "(col1 = 1 OR (col2 = \"test\" OR col3 = false))", ""},
		{"Multiple AND/OR conditions", "col1 = 1 AND col2 = \"test\" OR col3 = false", tab, "((col1 = 1 AND col2 = \"test\") OR col3 = false)", ""},
		{"Multiple OR/AND conditions", "col1 = 1 OR col2 = \"test\" AND col3 = false", tab, "(col1 = 1 OR (col2 = \"test\" AND col3 = false))", ""},
	}

	for i, row := range testStruct {
		tlist := tk.Tokenize(row.Command)
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetWhereConditionsFunc(profile, tlist, row.TableD, row.ExpectedCond, row.ExpectedErr))

	}

}

func testGetWhereConditionsFunc(profile *sqprofile.SQProfile, tkns *tk.TokenList, tab *sqt.TableDef, expectedCond string, errTxt string) func(*testing.T) {
	return func(t *testing.T) {
		rTkns, rConds, err := cmd.GetWhereConditions(profile, tkns, tab)
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
		if rTkns.Len() != 0 {
			t.Error("All tokens should be consumed by test")
			return
		}
		if rConds.ToString() != expectedCond {
			t.Error(fmt.Sprintf("Expected Condition (%s) does not match returned Condition (%s)", expectedCond, rConds.ToString()))
		}

	}
}
