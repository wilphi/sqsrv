package cmd_test

import (
	"fmt"
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func testInsertIntoFunc(profile *sqprofile.SQProfile, d InsertIntoData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()

		var tab *sqtables.TableDef
		var err error
		var initPtrs []int64
		// Snapshot of data if we need to do comparison
		if d.TableName != "" {
			tab = sqtables.GetTable(profile, d.TableName)
			initPtrs, err = tab.GetRowPtrs(profile, nil, true)
			if err != nil {
				t.Errorf("Unable to get table data for %s", d.TableName)
				return
			}

		}
		tkns := tokens.Tokenize(d.Command)
		_, _, err = cmd.InsertInto(profile, tkns)
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Error(fmt.Sprintf("Unexpected Error in test: %s", err.Error()))
				return
			}
			if d.ExpErr != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", d.ExpErr, err.Error()))
				return
			}
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", d.ExpErr))
			return
		}
		if d.TableName != "" {
			afterPtrs, err := tab.GetRowPtrs(profile, nil, true)
			if err != nil {
				t.Errorf("Unable to get table data for %s", d.TableName)
				return
			}
			ptrs := NotIn(afterPtrs, initPtrs)

			data, err := tab.GetRowDataFromPtrs(profile, ptrs)
			if err != nil {
				t.Errorf("Unable to get table data for %s", d.TableName)
				return
			}
			expVals := sqtypes.CreateValuesFromRaw(d.ExpVals)
			if !reflect.DeepEqual(expVals, data.Vals) {
				t.Error("Expected values do not match actual values")
				return
			}
		}
	}
}

// NotIn returns all items in A that are not in B
func NotIn(a, b []int64) []int64 {
	var ret []int64
	for _, x := range a {
		if !Contain(b, x) {
			ret = append(ret, x)
		}
	}
	return ret
}
func Contain(arr []int64, item int64) bool {
	for _, x := range arr {
		if x == item {
			return true
		}
	}
	return false
}

type InsertIntoData struct {
	TestName  string
	Command   string
	ExpErr    string
	ExpVals   sqtypes.RawVals
	TableName string
}

func TestInsertInto(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	//make sure table exists for testing
	tkns := tokens.Tokenize("CREATE TABLE instest (col1 int, col2 string, col3 bool, col4 float)")
	tableName, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for TestInsertInto: %s", err)
		return
	}

	data := []InsertIntoData{
		{
			TestName: "Missing Insert",
			Command:  "FROM",
			ExpErr:   "Error: Expecting INSERT INTO to start the statement",
		},
		{
			TestName: "INSERT ONLY",
			Command:  "INSERT",
			ExpErr:   "Error: Expecting INSERT INTO to start the statement",
		},
		{
			TestName: "INSERT Missing tableName",
			Command:  "INSERT INTO",
			ExpErr:   "Syntax Error: Expecting name of table for insert",
		},
		{
			TestName: "INSERT missing (",
			Command:  "INSERT INTO instest",
			ExpErr:   "Syntax Error: Expecting ( after name of table",
		},
		{
			TestName: "INSERT missing column",
			Command:  "INSERT INTO instest (",
			ExpErr:   "Syntax Error: Expecting name of column",
		},
		{
			TestName: "INSERT missing comma after col",
			Command:  "INSERT INTO instest (col1",
			ExpErr:   "Syntax Error: Comma is required to separate columns",
		},
		{
			TestName: "INSERT missing second column",
			Command:  "INSERT INTO instest (col1,",
			ExpErr:   "Syntax Error: Expecting name of column",
		},
		{
			TestName: "INSERT missing VALUES",
			Command:  "INSERT INTO instest (col1,col2,col3)",
			ExpErr:   "Syntax Error: Expecting keyword VALUES",
		},
		{
			TestName: "INSERT missing ( after values",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES",
			ExpErr:   "Syntax Error: Expecting ( after keyword VALUES",
		},
		{
			TestName: "INSERT missing value for col1",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (",
			ExpErr:   "Syntax Error: No values defined",
		},
		{
			TestName: "INSERT missing comma after first value",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123",
			ExpErr:   "Syntax Error: Comma is required to separate values",
		},
		{
			TestName: "INSERT missing value for col2",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123, ",
			ExpErr:   "Syntax Error: Expecting value or a valid expression",
		},
		{
			TestName: "INSERT missing value for col3",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", ",
			ExpErr:   "Syntax Error: Expecting value or a valid expression",
		},
		{
			TestName: "INSERT missing final )",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true",
			ExpErr:   "Syntax Error: Comma is required to separate values",
		},
		{
			TestName: "INSERT invalid after values section",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true) (",
			ExpErr:   "Syntax Error: Unexpected tokens after the values section: (",
		},
		{
			TestName: "INSERT missing ( for start of next value",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true), test",
			ExpErr:   "Syntax Error: Expecting ( to start next row of VALUES",
		},
		{
			TestName:  "INSERT three values",
			Command:   "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true)",
			ExpErr:    "",
			ExpVals:   sqtypes.RawVals{{123, "With Cols Test", true, nil}},
			TableName: tableName,
		},
		{
			TestName: "Extra comma in Column list",
			Command:  "INSERT INTO instest (col1,col2,col3,) VALUES (123, \"With Cols Test\", true)",
			ExpErr:   "Syntax Error: Unexpected \",\" before \")\"",
		},
		{
			TestName: "No Cols in Column list",
			Command:  "INSERT INTO instest () VALUES (123, \"With Cols Test\", true)",
			ExpErr:   "Syntax Error: No columns defined for table",
		},
		{
			TestName: "Extra comma in value list",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true,)",
			ExpErr:   "Syntax Error: Unexpected \",\" before \")\"",
		},
		{
			TestName: "No Vals in Value list",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES ()",
			ExpErr:   "Syntax Error: No values defined",
		},
		{
			TestName: "Cols do not match Values",
			Command:  "INSERT INTO instest (col1,col2) VALUES (123, \"With Cols Test\", true)",
			ExpErr:   "Error: The Number of Columns (2) does not match the number of Values (3)",
		},
		{
			TestName: "Values do not match Cols",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\")",
			ExpErr:   "Error: The Number of Columns (3) does not match the number of Values (2)",
		},
		{
			TestName: "Value Type does not match Col Type",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", 1234)",
			ExpErr:   "Error: Type Mismatch: Column col3 in Table instest has a type of BOOL, Unable to set value of type INT",
		},
		{
			TestName: "Insert target table does not exist",
			Command:  "INSERT INTO NotATable (col1,col2,col3) VALUES (123, \"With Cols Test\", true)",
			ExpErr:   "Error: Table NotATable does not exist",
		},
		{
			TestName: "More Cols than in table",
			Command:  "INSERT INTO instest (col1,col2,col3, colx) VALUES (123, \"With Cols Test\", true, \"Col does not exist\")",
			ExpErr:   "Error: Table instest does not have a column named colx",
		},
		{
			TestName: "Col does not exist in table",
			Command:  "INSERT INTO instest (col1,col2, colx) VALUES (123, \"With Cols Test\", \"Col does not exist\")",
			ExpErr:   "Error: Table instest does not have a column named colx",
		},
		{
			TestName: "Integer too large - tests invalid converion",
			Command:  "INSERT INTO instest (col1,col2,col3) VALUES (999999999999999999999, \"With Cols Test\", true)",
			ExpErr:   "Error: Type Mismatch: Column col1 in Table instest has a type of INT, Unable to set value of type FLOAT",
		},
		{
			TestName: "Muli row insert (3)",
			Command: "INSERT INTO instest (col1,col2,col3) VALUES " +
				fmt.Sprintf("(%d, %q, %t), ", 123, "With Cols Test", true) +
				fmt.Sprintf("(%d, %q, %t), ", 456, "Second Value Test", true) +
				fmt.Sprintf("(%d, %q, %t) ", 789, "Third Value Test", false),
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true, nil},
				{456, "Second Value Test", true, nil},
				{789, "Third Value Test", false, nil},
			},
			TableName: tableName,
		},
		{
			TestName: "Count in Insert",
			Command:  "INSERT INTO instest (col1, col2, count()) values (123, \"test count\", true)",
			ExpErr:   "Syntax Error: Expecting name of column",
		},
		{
			TestName:  "Null in Insert",
			Command:   "INSERT INTO instest (col1, col2, col3) values (123, null, true)",
			ExpErr:    "",
			ExpVals:   sqtypes.RawVals{{123, nil, true, nil}},
			TableName: tableName,
		},
		{
			TestName:  "INSERT Negative Number",
			Command:   "INSERT INTO instest (col1,col2,col3,col4) VALUES (123, \"With Cols Test\", true, -3.145)",
			ExpErr:    "",
			ExpVals:   sqtypes.RawVals{{123, "With Cols Test", true, -3.145}},
			TableName: tableName,
		},
		{
			TestName:  "INSERT More Values than cols",
			Command:   "INSERT INTO instest (col1,col2,col3) VALUES (123, \"With Cols Test\", true, -3.145)",
			ExpErr:    "Error: The Number of Columns (3) does not match the number of Values (4)",
			ExpVals:   sqtypes.RawVals{{123, "With Cols Test", true, -3.145}},
			TableName: tableName,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testInsertIntoFunc(profile, row))

	}

}
