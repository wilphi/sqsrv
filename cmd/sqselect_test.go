package cmd_test

import (
	"fmt"
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("cmd_test.log")
}

type SelectData struct {
	TestName string
	Command  string
	ExpErr   string
	ExpRows  int
	ExpCols  []string
	ExpVals  sqtypes.RawVals
}

func testSelectFunc(profile *sqprofile.SQProfile, d SelectData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		tkns := tokens.Tokenize(d.Command)
		_, data, err := cmd.Select(profile, tkns)
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
		if data.Len() != d.ExpRows {
			t.Errorf("The number of rows returned (%d) does not match expected rows (%d)", data.Len(), d.ExpRows)
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}
		if d.ExpCols == nil && data.GetColNames() != nil {
			t.Errorf("Expecting nil columns but got %d of them", data.NumCols())
			return
		}
		if data.NumCols() != len(d.ExpCols) {
			fmt.Println("Expected: ", d.ExpCols)
			fmt.Println("Result: ", data.GetColNames())
			t.Errorf("Number of columns returned (%d) does not match expected number of cols(%d)", data.NumCols(), len(d.ExpCols))
			return
		}
		for i, colName := range data.GetColNames() {
			if d.ExpCols[i] != colName {
				t.Errorf("Expecting col named (%s) but returned (%s) instead", d.ExpCols[i], colName)
				return
			}
		}
		if d.ExpVals != nil {
			expVals := sqtypes.CreateValuesFromRaw(d.ExpVals)
			if !reflect.DeepEqual(expVals, data.Vals) {
				t.Error("Expected Values and Actual values do not match")
				return
			}
		}
	}
}

func TestSelect(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Make sure datasets are by default in RowID order
	sqtables.RowOrder = true

	//make sure table exists for testing
	tkns := tokens.Tokenize("CREATE TABLE seltest (col1 int, col2 string, col3 bool)")
	_, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for TestSelect: %s", err)
		return
	}

	// Test to see what happens with empty table
	tkns = tokens.Tokenize("CREATE TABLE selEmpty (col1 int, col2 string, col3 bool)")
	_, err = cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for TestSelect: %s", err)
		return
	}

	testData := "INSERT INTO seltest (col1, col2, col3) VALUES " +
		"(123, \"With Cols Test\", true), " +
		"(456, \"Seltest 2\", true), " +
		"(789, \"Seltest 3\", false)"
	tkns = tokens.Tokenize(testData)
	if _, _, err := cmd.InsertInto(profile, tkns); err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	data := []SelectData{

		{
			TestName: "Select from empty table",
			Command:  "SELECT col1, col2, col3 from selEmpty",
			ExpErr:   "",
			ExpRows:  0,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals:  sqtypes.RawVals{},
		},
		{
			TestName: "SELECT Where invalid",
			Command:  "SELECT col1 FROM seltest WHERE col1=9999999999999999999999",
			ExpErr:   "Error: Type Mismatch: 1E+22 is not an Int",
			ExpRows:  0,
			ExpCols:  []string{"col1"},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT only",
			Command:  "SELECT",
			ExpErr:   "Syntax Error: No columns defined for query",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT missing comma",
			Command:  "SELECT col1",
			ExpErr:   "Syntax Error: Comma is required to separate columns",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT missing FROM",
			Command:  "SELECT col1, col2, col3",
			ExpErr:   "Syntax Error: Comma is required to separate columns",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT missing Table Name",
			Command:  "SELECT col1, col2, col3 FROM",
			ExpErr:   "Syntax Error: Expecting table name in select statement",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT from seltest",
			Command:  "SELECT col1, col2, col3 FROM seltest",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT * from seltest",
			Command:  "SELECT * FROM seltest",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "Invalid table name",
			Command:  "SELECT col1, col2 FROM NotATable",
			ExpErr:   "Error: Table NotATable does not exist for select statement",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "Invalid column name",
			Command:  "SELECT col1, col2, colx FROM seltest",
			ExpErr:   "Error: Table seltest does not have a column named colx",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT * tableName",
			Command:  "SELECT * seltest",
			ExpErr:   "Syntax Error: Expecting FROM",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "Select * from NotATable",
			Command:  "Select * from NotATable",
			ExpErr:   "Error: Table NotATable does not exist for select statement",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT too many columns",
			Command:  "SELECT col1, col2, col3, colx FROM seltest",
			ExpErr:   "Error: Table seltest does not have a column named colx",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT Where",
			Command:  "SELECT col1 FROM seltest WHERE col1=456",
			ExpErr:   "",
			ExpRows:  1,
			ExpCols:  []string{"col1"},
			ExpVals:  sqtypes.RawVals{{456}},
		},
		{
			TestName: "SELECT COUNT",
			Command:  "SELECT COUNT FROM seltest",
			ExpErr:   "Syntax Error: Count must be followed by ()",
			ExpRows:  0,
			ExpCols:  []string{"COUNT"},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT COUNT(",
			Command:  "SELECT COUNT( FROM seltest",
			ExpErr:   "Syntax Error: Count must be followed by ()",
			ExpRows:  0,
			ExpCols:  []string{"COUNT"},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT COUNT)",
			Command:  "SELECT COUNT) FROM seltest",
			ExpErr:   "Syntax Error: Count must be followed by ()",
			ExpRows:  0,
			ExpCols:  []string{"COUNT"},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT COUNT()",
			Command:  "SELECT COUNT() FROM seltest",
			ExpErr:   "",
			ExpRows:  1,
			ExpCols:  []string{"count()"},
			ExpVals:  sqtypes.RawVals{{3}},
		},
		{
			TestName: "SELECT COUNT(), Extra Col",
			Command:  "SELECT COUNT(), id FROM seltest",
			ExpErr:   "Syntax Error: Select Statements with Count() must not have other expressions",
			ExpRows:  1,
			ExpCols:  []string{"count()"},
			ExpVals:  sqtypes.RawVals{{3}},
		},
		{
			TestName: "SELECT Order BY",
			Command:  "SELECT * FROM seltest ORDER BY col1",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Order BY err",
			Command:  "SELECT * FROM seltest ORDER BY col1, dec",
			ExpErr:   "Error: Column dec not found in dataset",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Order BY missing comma",
			Command:  "SELECT * FROM seltest ORDER BY col1 col2",
			ExpErr:   "Syntax Error: Missing comma in ORDER BY clause",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Order BY DESC",
			Command:  "SELECT * FROM seltest ORDER BY col1 desc",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{789, "Seltest 3", false},
				{456, "Seltest 2", true},
				{123, "With Cols Test", true},
			},
		},
		{
			TestName: "SELECT Where & Order BY",
			Command:  "SELECT * FROM seltest WHERE col3 = true ORDER BY col1",
			ExpErr:   "",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT Order BY & Where",
			Command:  "SELECT * FROM seltest ORDER BY col1 WHERE col3 = true ",
			ExpErr:   "",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT Order BY & Where + Extra stuff",
			Command:  "SELECT * FROM seltest ORDER BY col1 WHERE col3 = true extra stuff",
			ExpErr:   "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT Order BY + Extra stuff",
			Command:  "SELECT * FROM seltest ORDER BY col1 extra stuff",
			ExpErr:   "Syntax Error: Missing comma in ORDER BY clause",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT + Extra stuff",
			Command:  "SELECT * FROM seltest extra stuff",
			ExpErr:   "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT col1*10 ",
			Command:  "SELECT col1*10, col2, col3 FROM seltest",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"(col1*10)", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Double Where ",
			Command:  "SELECT col1 FROM seltest WHERE col1 = 456 Where col1=123",
			ExpErr:   "Syntax Error: Duplicate where clause, only one allowed",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Double Order By ",
			Command:  "SELECT col1 FROM seltest Order by col1 Order by col1 desc",
			ExpErr:   "Syntax Error: Duplicate order by clause, only one allowed",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where expression err ",
			Command:  "SELECT col1 FROM seltest Where col1<",
			ExpErr:   "Syntax Error: Unexpected end to expression",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where expression err invalid col ",
			Command:  "SELECT col1 FROM seltest Where colX<5",
			ExpErr:   "Error: Table seltest does not have a column named colX",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where expression err reduce ",
			Command:  "SELECT col1 FROM seltest Where colX<(5-\"test\")",
			ExpErr:   "Error: Type Mismatch: test is not an Int",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSelectFunc(profile, row))

	}
}

func TestSelectExecute(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	t.Run("Invalid Table", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		data, err := cmd.SelectExecute(profile, "NotATable", &sqtables.ExprList{}, nil, nil)
		if err != nil && err.Error() != "Error: Table NotATable does not exist for select statement" {
			t.Errorf("Unexpected Error: %s", err)
			return

		}
		// Avoids unused variable
		if data != nil {
			data.Len()
		}
	})
}
