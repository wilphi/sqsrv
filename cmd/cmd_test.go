package cmd_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"

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

type GetIdentListData struct {
	TestName     string
	Terminator   *tk.Token
	Command      string
	ExpErr       string
	ExpectedCols []string
}

func testGetIdentListFunc(d GetIdentListData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		tkns := tk.Tokenize(d.Command)

		rTkns, rIdents, err := cmd.GetIdentList(tkns, d.Terminator, false)
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
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}
		if rTkns.Len() != 0 {
			t.Error("All tokens should be consumed by test")
			return
		}
		if len(d.ExpectedCols) != len(rIdents) {
			t.Errorf("The length Expected Idents (%d) and returned Idents (%d) do not match", len(d.ExpectedCols), len(rIdents))
			return
		}
		for !reflect.DeepEqual(d.ExpectedCols, rIdents) {
			t.Error("Expected Columns do not match actual Columns")
		}

	}
}
func TestGetIdentList(t *testing.T) {

	data := []GetIdentListData{
		{
			TestName:     "One Col",
			Terminator:   tk.SYMBOLS[')'],
			Command:      "col1",
			ExpErr:       "Syntax Error: Comma is required to separate column definitions",
			ExpectedCols: nil,
		},
		{
			TestName:     "Expect another Col",
			Terminator:   tk.SYMBOLS[')'],
			Command:      "col1,",
			ExpErr:       "Syntax Error: Expecting name of column",
			ExpectedCols: nil,
		},
		{
			TestName:     "Two Col",
			Terminator:   tk.SYMBOLS[')'],
			Command:      "col1, col2",
			ExpErr:       "Syntax Error: Comma is required to separate column definitions",
			ExpectedCols: nil,
		},
		{
			TestName:     "Expect a third Col",
			Terminator:   tk.SYMBOLS[')'],
			Command:      "col1,col2,",
			ExpErr:       "Syntax Error: Expecting name of column",
			ExpectedCols: nil,
		},
		{
			TestName:     "Three Col",
			Terminator:   tk.SYMBOLS[')'],
			Command:      "col1, col2, col3",
			ExpErr:       "Syntax Error: Comma is required to separate column definitions",
			ExpectedCols: nil,
		},
		{
			TestName:     "Complete col definition with )",
			Terminator:   tk.SYMBOLS[')'],
			Command:      "col1, col2, col3)",
			ExpErr:       "",
			ExpectedCols: []string{"col1", "col2", "col3"},
		},
		{
			TestName:     "Complete col definition with FROM",
			Terminator:   tk.Words[tk.From],
			Command:      "firstname, lastname, phonenum FROM",
			ExpErr:       "",
			ExpectedCols: []string{"firstname", "lastname", "phonenum"},
		},
		{
			TestName:     "Extra Comma in list",
			Terminator:   tk.SYMBOLS[')'],
			Command:      "col1, col2, col3,)",
			ExpErr:       "Syntax Error: Unexpected \",\" before \")\"",
			ExpectedCols: []string{"col1", "col2", "col3"},
		},
		{
			TestName:     "No Cols in list",
			Terminator:   tk.SYMBOLS[')'],
			Command:      ")",
			ExpErr:       "Syntax Error: No columns defined for table",
			ExpectedCols: []string{"col1", "col2", "col3"},
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetIdentListFunc(row))

	}

}

type GetWhereConditionsData struct {
	TestName     string
	Command      string
	TableName    string
	ExpectedCond string
	ExpErr       string
}

func TestGetWhereConditions(t *testing.T) {

	//make sure table exists for testing
	profile := sqprofile.CreateSQProfile()
	tkns := tokens.Tokenize("CREATE TABLE WhereCondtest (col1 int, col2 string, col3 bool)")
	tableName, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	data := []GetWhereConditionsData{
		{
			TestName:     "Equal Condition",
			Command:      "col1 = 1",
			TableName:    tableName,
			ExpectedCond: "col1 = 1",
			ExpErr:       "",
		},
		{
			TestName:     "Reverse Equal Condition",
			Command:      "1 = col1",
			TableName:    tableName,
			ExpectedCond: "col1 = 1",
			ExpErr:       "Syntax Error: Expecting a column name in where clause",
		},
		{
			TestName:     "No Col Condition",
			Command:      "2=1",
			TableName:    tableName,
			ExpectedCond: "",
			ExpErr:       "Syntax Error: Expecting a column name in where clause",
		},
		{
			TestName:     "Invalid Operator",
			Command:      "col1 ~ 1",
			TableName:    tableName,
			ExpectedCond: "col1 = 1",
			ExpErr:       "Syntax Error: Expecting an operator after column name (col1) in where clause",
		},
		{
			TestName:     "Missing Value",
			Command:      "col1 = ",
			TableName:    tableName,
			ExpectedCond: "col1 = 1",
			ExpErr:       "Syntax Error: Expecting a value in where clause after col1 =",
		},
		/* It is very difficult to get an invalid number due to the way parsing works
		{
			TestName:     "Invalid Value",
			Command:      "col1 = 999999999999999999999 ",
			TableName:    tableName,
			ExpectedCond: "col1 = 1",
			ExpErr:       "Syntax Error: \"9999999999999999999999\" is not a number",
		},*/
		{
			TestName:     "Simple AND Condition",
			Command:      "col1 = 1 AND col2 = \"test\"",
			TableName:    tableName,
			ExpectedCond: "(col1 = 1 AND col2 = \"test\")",
			ExpErr:       "",
		},
		{
			TestName:     "Simple OR Condition",
			Command:      "col1 = 1 OR col2 = \"test\"",
			TableName:    tableName,
			ExpectedCond: "(col1 = 1 OR col2 = \"test\")",
			ExpErr:       "",
		},
		{
			TestName:     "Multiple AND Conditions",
			Command:      "col1 = 1 AND col2 = \"test\" AND col3 = false",
			TableName:    tableName,
			ExpectedCond: "((col1 = 1 AND col2 = \"test\") AND col3 = false)",
			ExpErr:       "",
		},
		{
			TestName:     "Multiple OR Conditions",
			Command:      "col1 = 1 OR col2 = \"test\" OR col3 = false",
			TableName:    tableName,
			ExpectedCond: "(col1 = 1 OR (col2 = \"test\" OR col3 = false))",
			ExpErr:       "",
		},
		{
			TestName:     "Multiple AND/OR conditions",
			Command:      "col1 = 1 AND col2 = \"test\" OR col3 = false",
			TableName:    tableName,
			ExpectedCond: "((col1 = 1 AND col2 = \"test\") OR col3 = false)",
			ExpErr:       "",
		},
		{
			TestName:     "Multiple OR/AND conditions",
			Command:      "col1 = 1 OR col2 = \"test\" AND col3 = false",
			TableName:    tableName,
			ExpectedCond: "(col1 = 1 OR (col2 = \"test\" AND col3 = false))",
			ExpErr:       "",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetWhereConditionsFunc(profile, row))

	}

}

func testGetWhereConditionsFunc(profile *sqprofile.SQProfile, d GetWhereConditionsData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		tkns := tk.Tokenize(d.Command)
		tab := sqt.GetTable(profile, d.TableName)
		rTkns, rConds, err := cmd.GetWhereConditions(profile, tkns, tab)
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
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}
		if rTkns.Len() != 0 {
			t.Error("All tokens should be consumed by test")
			return
		}
		if rConds.ToString() != d.ExpectedCond {
			t.Errorf("Expected Condition (%s) does not match actual Condition (%s)", d.ExpectedCond, rConds.ToString())
		}

	}
}

func testOrderByFunc(profile *sqprofile.SQProfile, d OrderByData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		tkns := tokens.Tokenize(d.Command)

		aOrderBy, err := cmd.OrderByClause(tkns)
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
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}

		if !reflect.DeepEqual(aOrderBy, d.ExpOrder) {
			t.Error("Expected Order By Columns do not match actual columns")
		}
	}
}

type OrderByData struct {
	TestName string
	Command  string
	ExpErr   string
	ExpOrder []sqtables.OrderItem
}

func TestOrderBy(t *testing.T) {

	//make sure table exists for testing
	profile := sqprofile.CreateSQProfile()

	data := []OrderByData{
		{
			TestName: "Order By Three Cols",
			Command:  "Order by col1 asc, col2 desc, col3",
			ExpErr:   "",
			ExpOrder: []sqtables.OrderItem{
				{ColName: "col1", SortType: "ASC"},
				{ColName: "col2", SortType: "DESC"},
				{ColName: "col3", SortType: "ASC"},
			},
		},
		{
			TestName: "Order  no By Three Cols",
			Command:  "Order col1 asc, col2 desc, col3",
			ExpErr:   "Syntax Error: ORDER missing BY",
			ExpOrder: []sqtables.OrderItem{
				{ColName: "col1", SortType: "ASC"},
				{ColName: "col2", SortType: "DESC"},
				{ColName: "col3", SortType: "ASC"},
			},
		},
		{
			TestName: "Order By followed by Where",
			Command:  "Order By col1 asc, col2 desc, col3 Where",
			ExpErr:   "",
			ExpOrder: []sqtables.OrderItem{
				{ColName: "col1", SortType: "ASC"},
				{ColName: "col2", SortType: "DESC"},
				{ColName: "col3", SortType: "ASC"},
			},
		},
		{
			TestName: "Order By single col",
			Command:  "Order By col1 ",
			ExpErr:   "",
			ExpOrder: []sqtables.OrderItem{
				{ColName: "col1", SortType: "ASC"},
			},
		},
		{
			TestName: "Order By hanging comma",
			Command:  "Order By col1, ",
			ExpErr:   "Syntax Error: Missing column name in ORDER BY clause",
			ExpOrder: []sqtables.OrderItem{
				{ColName: "col1", SortType: "ASC"},
			},
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testOrderByFunc(profile, row))

	}
}
