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
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

type UpdateData struct {
	TestName  string
	SQLStr    string
	TableName string
	Cols      []string
	ExpErr    string
	ExpData   sqtypes.RawVals
}

func TestUpdate(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tkns := tokens.Tokenize("CREATE TABLE testupdate (col1 int not null, col2 string)")
	_, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for TestUpdate: %s", err)
		return
	}
	ins := "INSERT INTO testupdate (col1, col2) VALUES " +
		"(1, \"test row 1\"), " +
		"(2, \"test row 2\"), " +
		"(3, \"test row 3\"), " +
		"(4, \"test row 4\"), " +
		"(5, \"test row 5\"), " +
		"(6, \"test row 6\")"
	tkns = tokens.Tokenize(ins)
	_, _, err = cmd.InsertInto(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for TestUpdate: %s", err)
		return
	}

	data := []UpdateData{

		{
			TestName: "UPDATE only",
			SQLStr:   "UPDATE",
			ExpErr:   "Syntax Error: Expecting table name in Update statement",
		},
		{
			TestName: "UPDATE Missing SET",
			SQLStr:   "UPDATE testupdate",
			ExpErr:   "Syntax Error: Expecting SET",
		},
		{
			TestName: "UPDATE Missing Ident",
			SQLStr:   "UPDATE testupdate SET ",
			ExpErr:   "Syntax Error: Expecting valid SET expression",
		},
		{
			TestName: "UPDATE Missing = ",
			SQLStr:   "UPDATE testupdate SET col1",
			ExpErr:   "Syntax Error: Expecting = after column name col1 in UPDATE SET",
		},
		{
			TestName: "UPDATE Missing Value ",
			SQLStr:   "UPDATE testupdate SET col1 =",
			ExpErr:   "Syntax Error: Expecting a value in SET clause after col1 =",
		},
		{
			TestName:  "UPDATE with Where Clause",
			SQLStr:    "UPDATE testupdate SET col1 = 99 WHERE col2 = \"test row 4\"",
			ExpErr:    "",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{1, "test row 1"}, {2, "test row 2"}, {3, "test row 3"}, {5, "test row 5"}, {6, "test row 6"}, {99, "test row 4"}},
		},
		{
			TestName:  "UPDATE No Where Clause",
			SQLStr:    "UPDATE testupdate SET col1 = 99",
			ExpErr:    "",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{99, "test row 1"}, {99, "test row 2"}, {99, "test row 3"}, {99, "test row 4"}, {99, "test row 5"}, {99, "test row 6"}},
		},
		{
			TestName:  "UPDATE Invalid table",
			SQLStr:    "UPDATE testupdate99 SET col1 = 99",
			ExpErr:    "Syntax Error: Invalid table name: testupdate99 does not exist",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{99, "test row 1"}, {99, "test row 2"}, {99, "test row 3"}, {99, "test row 4"}, {99, "test row 5"}, {99, "test row 6"}},
		},
		{
			TestName:  "UPDATE Invalid Col",
			SQLStr:    "UPDATE testupdate SET colX = 99",
			ExpErr:    "Syntax Error: Invalid Column name: colX does not exist in Table testupdate",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{99, "test row 1"}, {99, "test row 2"}, {99, "test row 3"}, {99, "test row 4"}, {99, "test row 5"}, {99, "test row 6"}},
		},
		{
			TestName:  "UPDATE Invalid Value",
			SQLStr:    "UPDATE testupdate SET col1 = 9999999999999999999999999",
			ExpErr:    "Error: Type Mismatch: Column col1 in Table testupdate has a type of INT, Unable to set value of type FLOAT",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{99, "test row 1"}, {99, "test row 2"}, {99, "test row 3"}, {99, "test row 4"}, {99, "test row 5"}, {99, "test row 6"}},
		},
		{
			TestName:  "UPDATE Invalid Value Type",
			SQLStr:    "UPDATE testupdate SET col1 = \"TEST\"",
			ExpErr:    "Error: Type Mismatch: Column col1 in Table testupdate has a type of INT, Unable to set value of type STRING",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{99, "test row 1"}, {99, "test row 2"}, {99, "test row 3"}, {99, "test row 4"}, {99, "test row 5"}, {99, "test row 6"}},
		},
		{
			TestName:  "UPDATE with error in Where Clause",
			SQLStr:    "UPDATE testupdate SET col1 = 99 WHERE col1 = \"test row 4\"",
			ExpErr:    "Error: Type Mismatch in Where clause expression: col1(INT) = test row 4(STRING)",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{1, "test row 1"}, {2, "test row 2"}, {3, "test row 3"}, {5, "test row 5"}, {6, "test row 6"}, {99, "test row 4"}},
		},
		{
			TestName:  "UPDATE with Where Clause + extra stuff",
			SQLStr:    "UPDATE testupdate SET col1 = 99 WHERE col1 = 34 extra stuff",
			ExpErr:    "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{1, "test row 1"}, {2, "test row 2"}, {3, "test row 3"}, {5, "test row 5"}, {6, "test row 6"}, {99, "test row 4"}},
		},
		{
			TestName:  "UPDATE + extra stuff",
			SQLStr:    "UPDATE testupdate SET col1 = 99 extra stuff",
			ExpErr:    "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{1, "test row 1"}, {2, "test row 2"}, {3, "test row 3"}, {5, "test row 5"}, {6, "test row 6"}, {99, "test row 4"}},
		},
		{
			TestName:  "UPDATE multi cols",
			SQLStr:    "UPDATE testupdate SET col1 = 99, col2 = \"test multi\"",
			ExpErr:    "",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{99, "test multi"}, {99, "test multi"}, {99, "test multi"}, {99, "test multi"}, {99, "test multi"}, {99, "test multi"}},
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
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		profile := sqprofile.CreateSQProfile()
		tkns := tokens.Tokenize(d.SQLStr)
		_, data, err := cmd.Update(profile, tkns)
		if data != nil {
			t.Errorf("Update returned a non nil dataset")
			return
		}
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if d.ExpErr != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", d.ExpErr, err.Error()))
				return
			}
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}
		if d.ExpData != nil {
			cList := sqtables.NewColListNames(d.Cols)
			tab := sqtables.GetTable(profile, d.TableName)
			ds, err := tab.GetRowData(profile, cList, nil)
			if err != nil {
				t.Errorf("Error getting data for comparison: %s", err)
				return
			}
			v := ds.Vals
			sort.SliceStable(v, func(i, j int) bool { return v[i][1].LessThan(v[j][1]) })
			sort.SliceStable(v, func(i, j int) bool { return v[i][0].LessThan(v[j][0]) })

			if !reflect.DeepEqual(v, sqtypes.CreateValuesFromRaw(d.ExpData)) {
				t.Error("Expected data does not match actual data in table")
				return
			}
		}
	}
}
