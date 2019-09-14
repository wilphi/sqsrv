package cmd_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("cmd_test.log")
}

type GetIdentListData struct {
	TestName     string
	Terminator   string
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
		tkns := tokens.Tokenize(d.Command)

		rIdents, err := cmd.GetIdentList(tkns, d.Terminator)
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
		if tkns.Len() != 0 {
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
			Terminator:   tokens.CloseBracket,
			Command:      "col1",
			ExpErr:       "Syntax Error: Comma is required to separate columns",
			ExpectedCols: nil,
		},
		{
			TestName:     "Expect another Col",
			Terminator:   tokens.CloseBracket,
			Command:      "col1,",
			ExpErr:       "Syntax Error: Expecting name of column",
			ExpectedCols: nil,
		},
		{
			TestName:     "Two Col",
			Terminator:   tokens.CloseBracket,
			Command:      "col1, col2",
			ExpErr:       "Syntax Error: Comma is required to separate columns",
			ExpectedCols: nil,
		},
		{
			TestName:     "Expect a third Col",
			Terminator:   tokens.CloseBracket,
			Command:      "col1,col2,",
			ExpErr:       "Syntax Error: Expecting name of column",
			ExpectedCols: nil,
		},
		{
			TestName:     "Three Col",
			Terminator:   tokens.CloseBracket,
			Command:      "col1, col2, col3",
			ExpErr:       "Syntax Error: Comma is required to separate columns",
			ExpectedCols: nil,
		},
		{
			TestName:     "Complete col definition with )",
			Terminator:   tokens.CloseBracket,
			Command:      "col1, col2, col3)",
			ExpErr:       "",
			ExpectedCols: []string{"col1", "col2", "col3"},
		},
		{
			TestName:     "Complete col definition with FROM",
			Terminator:   tokens.From,
			Command:      "firstname, lastname, phonenum FROM",
			ExpErr:       "",
			ExpectedCols: []string{"firstname", "lastname", "phonenum"},
		},
		{
			TestName:     "Extra Comma in list",
			Terminator:   tokens.CloseBracket,
			Command:      "col1, col2, col3,)",
			ExpErr:       "Syntax Error: Unexpected \",\" before \")\"",
			ExpectedCols: []string{"col1", "col2", "col3"},
		},
		{
			TestName:     "No Cols in list",
			Terminator:   tokens.CloseBracket,
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

/////////////////////////////////////////////////////////////////////////////////////////////////////////

type GetExprData struct {
	TestName   string
	Terminator string
	Command    string
	ExpErr     string
	ExpExpr    string
	ValuesOnly bool
}

func testGetExprFunc(d GetExprData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		tkns := tokens.Tokenize(d.Command)

		actExpr, err := cmd.GetExpr(tkns, nil, 0, d.Terminator)
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
		if d.ExpExpr != actExpr.Name() {
			t.Errorf("Expected Expressions %s do not match actual Expressions %s", d.ExpExpr, actExpr.Name())
			return
		}
	}
}

func TestGetExpr(t *testing.T) {

	data := []GetExprData{
		{
			TestName:   "Full Where expression",
			Terminator: tokens.Order,
			Command:    "col1 = -1 and col2 = \"test\" or col1=3 and col2 = \"nat\"",
			ExpErr:     "",
			ExpExpr:    "(((col1=(-1))AND(col2=test))OR((col1=3)AND(col2=nat)))",
		},

		{
			TestName:   "Full expression <= =>, !=",
			Terminator: tokens.Order,
			Command:    "col1 <= 1 and col2 != \"test\" or col1>=3 and col2 = \"nat\"",
			ExpErr:     "",
			ExpExpr:    "(((col1<=1)AND(col2!=test))OR((col1>=3)AND(col2=nat)))",
		},
		{
			TestName:   "Expression with FLOAT function",
			Terminator: tokens.Order,
			Command:    "FLOAT(col1) <= 1.0",
			ExpErr:     "",
			ExpExpr:    "(FLOAT(col1)<=1)",
		},
		{
			TestName:   "Expression with FLOAT error no (",
			Terminator: tokens.Order,
			Command:    "FLOAT col1  <= 1.0",
			ExpErr:     "Syntax Error: Function FLOAT must be followed by (",
			ExpExpr:    "(FLOAT(col1)<=1)",
		},
		{
			TestName:   "Expression with FLOAT error no )",
			Terminator: tokens.Order,
			Command:    "FLOAT(col1  <= 1.0",
			ExpErr:     "Syntax Error: Function FLOAT is missing ) after expression",
			ExpExpr:    "(FLOAT(col1)<=1)",
		},
		{
			TestName:   "Expression with FLOAT error no expression in()",
			Terminator: tokens.Order,
			Command:    "FLOAT() <= 1.0",
			ExpErr:     "Syntax Error: Function FLOAT is missing an expression between ( and )",
			ExpExpr:    "(FLOAT(col1)<=1)",
		},
		{
			TestName:   "Expression with FLOAT function with err",
			Terminator: tokens.Order,
			Command:    "FLOAT(float) <= 1.0",
			ExpErr:     "Syntax Error: Function FLOAT must be followed by (",
			ExpExpr:    "(FLOAT(col1)<=1)",
		},

		{
			TestName:   "Count Expression",
			Terminator: tokens.From,
			Command:    "count()",
			ExpErr:     "",
			ExpExpr:    "count()",
		},
		{
			TestName:   "Count Err no brackets",
			Terminator: tokens.From,
			Command:    "count",
			ExpErr:     "Syntax Error: Count must be followed by ()",
			ExpExpr:    "count()",
		},
		{
			TestName:   "Count Err Open bracket",
			Terminator: tokens.From,
			Command:    "count(",
			ExpErr:     "Syntax Error: Count must be followed by ()",
			ExpExpr:    "count()",
		},
		{
			TestName:   "Count Err Close bracket",
			Terminator: tokens.From,
			Command:    "count)",
			ExpErr:     "Syntax Error: Count must be followed by ()",
			ExpExpr:    "count()",
		},
		{
			TestName:   "Count Err Expression",
			Terminator: tokens.From,
			Command:    "count(x=1)",
			ExpErr:     "Syntax Error: Count must be followed by ()",
			ExpExpr:    "count()",
		},
		{
			TestName:   "Err Expression",
			Terminator: tokens.From,
			Command:    "a+ by",
			ExpErr:     "Syntax Error: Invalid expression: Unable to find a value or column near BY",
			ExpExpr:    "count()",
		},
		{
			TestName:   "Brackets Expression",
			Terminator: tokens.From,
			Command:    "8/2*(2+2)",
			ExpErr:     "",
			ExpExpr:    "((8/2)*(2+2))",
		},
		{
			TestName:   "Brackets Expression 2",
			Terminator: tokens.From,
			Command:    "8/(2+2)*2",
			ExpErr:     "",
			ExpExpr:    "((8/(2+2))*2)",
		},
		{
			TestName:   "Brackets Expression Missing End Bracket",
			Terminator: tokens.From,
			Command:    "8/(2+2*2",
			ExpErr:     "Syntax Error: '(' does not have a matching ')'",
			ExpExpr:    "((8/(2+2))*2)",
		},
		{
			TestName:   "Complex Brackets Expression",
			Terminator: tokens.From,
			Command:    "8/(2+((9-3)/1)*(2+1))*2",
			ExpErr:     "",
			ExpExpr:    "((8/(2+(((9-3)/1)*(2+1))))*2)",
		},
		{
			TestName:   "TableName.Col ",
			Terminator: tokens.From,
			Command:    "tablea.col FROM",
			ExpErr:     "",
			ExpExpr:    "tablea.col",
		},
		{
			TestName:   "TableName. missing col ",
			Terminator: tokens.From,
			Command:    "tablea. FROM",
			ExpErr:     "Syntax Error: Expecting column after tablea.",
			ExpExpr:    "tablea.col",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetExprFunc(row))

	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
		{
			TestName: "Order By missing comma",
			Command:  "Order By col1 col2 ",
			ExpErr:   "Syntax Error: Missing comma in ORDER BY clause",
			ExpOrder: []sqtables.OrderItem{
				{ColName: "col1", SortType: "ASC"},
			},
		},
		{
			TestName: "Order By Empty",
			Command:  "Order By ",
			ExpErr:   "Syntax Error: Missing column name in ORDER BY clause",
			ExpOrder: []sqtables.OrderItem{
				{ColName: "col1", SortType: "ASC"},
			},
		},
		{
			TestName: "Order By tablename only",
			Command:  "Order By tablea.  ",
			ExpErr:   "Syntax Error: Column name must follow tablea.",
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

/////////////////////////////////////////////////////////////////////////////////////////////////////////

type GetExprListData struct {
	TestName   string
	Terminator string
	Command    string
	ExpErr     string
	ExpExprTxt string
	ValuesOnly bool
}

func testGetExprListFunc(d GetExprListData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		tkns := tokens.Tokenize(d.Command)

		rExprs, err := cmd.GetExprList(tkns, d.Terminator, d.ValuesOnly)
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
		if tkns.Test(d.Terminator) == "" {
			t.Error("Remaining token should be Terminator")
			return
		}

		if d.ExpExprTxt != rExprs.ToString() {
			t.Errorf("Expected Expressions %v do not match actual Expressions %v", d.ExpExprTxt, rExprs.ToString())
			return
		}

	}
}
func TestGetExprList(t *testing.T) {

	data := []GetExprListData{
		{
			TestName:   "Invalid Expression",
			Terminator: tokens.CloseBracket,
			Command:    "~",
			ExpErr:     "Syntax Error: Invalid expression: Unable to find a value or column near ~",
			ExpExprTxt: "",
		},
		{
			TestName:   "One Col",
			Terminator: tokens.CloseBracket,
			Command:    "col1",
			ExpErr:     "Syntax Error: Comma is required to separate columns",
			ExpExprTxt: "",
		},
		{
			TestName:   "Expect another Col",
			Terminator: tokens.CloseBracket,
			Command:    "col1,",
			ExpErr:     "Syntax Error: Expecting name of column or a valid expression",
			ExpExprTxt: "",
		},
		{
			TestName:   "Two Col",
			Terminator: tokens.CloseBracket,
			Command:    "col1, col2",
			ExpErr:     "Syntax Error: Comma is required to separate columns",
			ExpExprTxt: "",
		},
		{
			TestName:   "Expect a third Col",
			Terminator: tokens.CloseBracket,
			Command:    "col1,col2,",
			ExpErr:     "Syntax Error: Expecting name of column or a valid expression",
			ExpExprTxt: "",
		},
		{
			TestName:   "Three Col",
			Terminator: tokens.CloseBracket,
			Command:    "col1, col2, col3",
			ExpErr:     "Syntax Error: Comma is required to separate columns",
			ExpExprTxt: "",
		},
		{
			TestName:   "Complete col definition with )",
			Terminator: tokens.CloseBracket,
			Command:    "col1, col2, col3)",
			ExpErr:     "",
			ExpExprTxt: "col1,col2,col3",
		},
		{
			TestName:   "Complete col definition with FROM",
			Terminator: tokens.From,
			Command:    "firstname, lastname, phonenum FROM",
			ExpErr:     "",
			ExpExprTxt: "firstname,lastname,phonenum",
		},
		{
			TestName:   "Extra Comma in list",
			Terminator: tokens.CloseBracket,
			Command:    "col1, col2, col3,)",
			ExpErr:     "Syntax Error: Unexpected \",\" before \")\"",
			ExpExprTxt: "",
		},
		{
			TestName:   "No Cols in list",
			Terminator: tokens.CloseBracket,
			Command:    ")",
			ExpErr:     "Syntax Error: No columns defined for query",
			ExpExprTxt: "",
		},
		{
			TestName:   "Value, col, OpExpr with FROM",
			Terminator: tokens.From,
			Command:    "1, lastname, \"Cell: \"+phonenum FROM",
			ExpErr:     "",
			ExpExprTxt: "1,lastname,(Cell: +phonenum)",
		},
		{
			TestName:   "Empty Expression",
			Terminator: tokens.CloseBracket,
			Command:    "1,,test)",
			ExpErr:     "Syntax Error: Expecting name of column or a valid expression",
			ExpExprTxt: "",
		},

		{
			TestName:   "Complex Expression",
			Terminator: tokens.From,
			Command:    "1, 1+2*3-8/4*3+9, lastname FROM",
			ExpErr:     "",
			ExpExprTxt: "1,10,lastname",
		},
		{
			TestName:   "Complex Expression 2",
			Terminator: tokens.From,
			Command:    "1, 4*3+9/3-18+75+1*9, lastname FROM",
			ExpErr:     "",
			ExpExprTxt: "1,81,lastname",
		},
		{
			TestName:   "Complex Col Expression",
			Terminator: tokens.From,
			Command:    "firstname, 3*id/2+12, lastname FROM",
			ExpErr:     "",
			ExpExprTxt: "firstname,(((3*id)/2)+12),lastname",
		},
		{
			TestName:   "Type Mismatch",
			Terminator: tokens.From,
			Command:    "1, 1+2*3+3.0, lastname FROM",
			ExpErr:     "Error: Type Mismatch: 3 is not an Int",
			ExpExprTxt: "",
		},
		{
			TestName:   "Type Mismatch String",
			Terminator: tokens.From,
			Command:    "1, \"Test\"+3.0, lastname FROM",
			ExpErr:     "Error: Type Mismatch: 3 is not a String",
			ExpExprTxt: "",
		},
		{
			TestName:   "Negative Number",
			Terminator: tokens.From,
			Command:    "1,1+-9, lastname FROM",
			ExpErr:     "",
			ExpExprTxt: "1,-8,lastname",
		},
		{
			TestName:   "Negative Number start",
			Terminator: tokens.From,
			Command:    "1,-9*2 +3*14, lastname FROM",
			ExpErr:     "",
			ExpExprTxt: "1,24,lastname",
		},
		{
			TestName:   "Negative column",
			Terminator: tokens.From,
			Command:    "1,-id, lastname FROM",
			ExpErr:     "",
			ExpExprTxt: "1,(-id),lastname",
		},
		{
			TestName:   "ValueOnly with col",
			Terminator: tokens.From,
			Command:    "1,-id, lastname FROM",
			ExpErr:     "Syntax Error: Expression \"(-id)\" did not reduce to a value",
			ExpExprTxt: "",
			ValuesOnly: true,
		},
		{
			TestName:   "ValueOnly with Negative Expression",
			Terminator: tokens.From,
			Command:    "1,-25/5*4 FROM",
			ExpErr:     "",
			ExpExprTxt: "1,-20",
			ValuesOnly: true,
		},
		{
			TestName:   "ValueOnly with Invalid  Expression",
			Terminator: tokens.From,
			Command:    "20/~, 1,-25/5*4 FROM",
			ExpErr:     "Syntax Error: Invalid expression: Unable to find a value or column near ~",
			ExpExprTxt: "",
			ValuesOnly: true,
		},
		{
			TestName:   "Partial  Expression",
			Terminator: tokens.From,
			Command:    "20+",
			ExpErr:     "Syntax Error: Unexpected end to expression",
			ExpExprTxt: "",
		},
		{
			TestName:   "Partial  Expression with Multiply",
			Terminator: tokens.From,
			Command:    "20+5*",
			ExpErr:     "Syntax Error: Unexpected end to expression",
			ExpExprTxt: "",
		},
		{
			TestName:   "Expression list includes int function",
			Terminator: tokens.From,
			Command:    "20+int(1.0), -20 FROM",
			ExpErr:     "",
			ExpExprTxt: "21,-20",
		},
		{
			TestName:   "Expression list includes int not bracket",
			Terminator: tokens.From,
			Command:    "20+int, -20 FROM",
			ExpErr:     "Syntax Error: Function INT must be followed by (",
			ExpExprTxt: "",
		},
		{
			TestName:   "Expression list includes int function incomplete",
			Terminator: tokens.From,
			Command:    "20+int(1.0, -20 FROM",
			ExpErr:     "Syntax Error: Function INT is missing ) after expression",
			ExpExprTxt: "",
		},
		{
			TestName:   "Expression list includes int function partial expression",
			Terminator: tokens.From,
			Command:    "20+int(1.0+), -20 FROM",
			ExpErr:     "Syntax Error: Invalid expression: Unable to find a value or column near )",
			ExpExprTxt: "",
		},
		{
			TestName:   "Expression list includes int, Float function",
			Terminator: tokens.From,
			Command:    "20+int(1.0) first, float(-20)+1.95 second FROM",
			ExpErr:     "",
			ExpExprTxt: "21 first,-18.05 second",
		},
		{
			TestName:   "Count with alias",
			Terminator: tokens.From,
			Command:    "count() Total FROM",
			ExpErr:     "",
			ExpExprTxt: "count() Total",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetExprListFunc(row))

	}

}

///////////////////////////////////////////////////////////////////////////////////////////

type GetTableListData struct {
	TestName       string
	Terminators    []string
	Command        string
	ExpErr         string
	ExpTab         *sqtables.TableList
	ExpectedTables []string
	ExpTokenLen    int
}

func testGetTableListFunc(d GetTableListData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		profile := sqprofile.CreateSQProfile()

		tkns := tokens.Tokenize(d.Command)

		rIdents, err := cmd.GetTableList(profile, tkns, d.Terminators...)
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
		if tkns.Len() != d.ExpTokenLen {
			t.Error("All tokens should be consumed by test")
			return
		}
		if len(d.ExpectedTables) != rIdents.Len() {
			t.Errorf("The length Expected TableNames (%d) and returned TableNames (%d) do not match", len(d.ExpectedTables), rIdents.Len())
			return
		}
		sort.Strings(d.ExpectedTables)
		if !reflect.DeepEqual(d.ExpectedTables, rIdents.TableNames()) {
			t.Error("Expected Tables do not match actual Tables")
			return
		}

	}
}
func TestGetTableList(t *testing.T) {
	////////////////////////////////////////////
	// Setup tables for tests
	////////////////////////////////////////////
	profile := sqprofile.CreateSQProfile()
	tableData := []struct {
		Name string
		Col  sqtables.ColDef
	}{
		{Name: "gettablelistTable1", Col: sqtables.CreateColDef("col1", "INT", false)},
		{Name: "gettablelistTable2", Col: sqtables.CreateColDef("col1", "INT", false)},
		{Name: "gettablelistTable3", Col: sqtables.CreateColDef("col1", "INT", false)},
		{Name: "gettablelistcountry", Col: sqtables.CreateColDef("col1", "INT", false)},
		{Name: "gettablelistcity", Col: sqtables.CreateColDef("col1", "INT", false)},
		{Name: "gettablelistperson", Col: sqtables.CreateColDef("col1", "INT", false)},
	}
	for _, tabDat := range tableData {
		tab := sqtables.CreateTableDef(tabDat.Name, tabDat.Col)
		err := sqtables.CreateTable(profile, tab)
		if err != nil {
			t.Errorf("Error setting up %s: %s", t.Name(), tabDat.Name)
			return
		}
	}

	data := []GetTableListData{
		{
			TestName:       "One Table",
			Terminators:    []string{tokens.CloseBracket},
			Command:        "gettablelistTable1",
			ExpErr:         "",
			ExpectedTables: []string{"gettablelistTable1"},
		},
		{
			TestName:       "Expect another Table",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistTable1,",
			ExpErr:         "Syntax Error: Unexpected ',' in From clause",
			ExpectedTables: nil,
		},
		{
			TestName:       "Two Tables",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistTable1, gettablelistTable2",
			ExpErr:         "",
			ExpectedTables: []string{"gettablelistTable1", "gettablelistTable2"},
		},
		{
			TestName:       "Expect a third Table",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistTable1,gettablelistTable2,",
			ExpErr:         "Syntax Error: Unexpected ',' in From clause",
			ExpectedTables: nil,
		},
		{
			TestName:       "Three Table",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistTable1, gettablelistTable2, gettablelistTable3",
			ExpErr:         "",
			ExpectedTables: []string{"gettablelistTable1", "gettablelistTable2", "gettablelistTable3"},
		},
		{
			TestName:       "Complete Table definition with Where",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistTable1, gettablelistTable2, gettablelistTable3 Where",
			ExpErr:         "",
			ExpectedTables: []string{"gettablelistTable1", "gettablelistTable2", "gettablelistTable3"},
			ExpTokenLen:    1,
		},
		{
			TestName:       "Complete Table definition with Order",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistcountry, gettablelistcity,gettablelistperson ORDER",
			ExpErr:         "",
			ExpectedTables: []string{"gettablelistcountry", "gettablelistcity", "gettablelistperson"},
			ExpTokenLen:    1,
		},
		{
			TestName:       "Extra Comma in list",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistTable1, gettablelistTable2, gettablelistTable3, Where",
			ExpErr:         "Syntax Error: Unexpected ',' in From clause",
			ExpectedTables: []string{"gettablelistTable1", "gettablelistTable2", "gettablelistTable3"},
		},
		{
			TestName:       "No Tables in list",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "Where",
			ExpErr:         "Syntax Error: No Tables defined for query",
			ExpectedTables: []string{"gettablelistTable1", "gettablelistTable2", "gettablelistTable3"},
		},
		{
			TestName:       "Not a tablename",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistTable1, () Where",
			ExpErr:         "Syntax Error: Expecting name of Table",
			ExpectedTables: []string{"gettablelistTable1", "gettablelistTable2", "gettablelistTable3"},
		},
		{
			TestName:       "Missing tablename",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        ", test Where",
			ExpErr:         "Syntax Error: Expecting name of Table",
			ExpectedTables: []string{"gettablelistTable1", "gettablelistTable2", "gettablelistTable3"},
		},
		{
			TestName:       "Missing comma",
			Terminators:    []string{tokens.Where, tokens.Order},
			Command:        "gettablelistTable1  alias1 gettablelistTable2 Where",
			ExpErr:         "Syntax Error: Comma is required to separate tables",
			ExpectedTables: []string{"gettablelistTable1", "gettablelistTable2", "gettablelistTable3"},
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetTableListFunc(row))

	}

}
