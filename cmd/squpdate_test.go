package cmd_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

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

type UpdateData struct {
	TestName  string
	SQLStr    string
	TableName string
	Cols      []string
	ExpErr    string
	ExpData   sqtypes.RawVals
	ResetData bool
}

func TestUpdate(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tkns := tokens.Tokenize("CREATE TABLE testupdate (col1 int not null, col2 string)")
	_, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for TestUpdate: %s", err)
		return
	}

	err = resetUpdateData()
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
			ExpErr:   "Syntax Error: Expecting an expression in SET clause after col1 =",
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
			ExpErr:    "Error: Type Mismatch: test row 4 is not an Int",
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
		{
			TestName:  "UPDATE Expressions",
			SQLStr:    "UPDATE testupdate SET col1 = col1+1, col2 = \"test \"+\"Expression\"",
			ExpErr:    "",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{2, "test Expression"}, {3, "test Expression"}, {4, "test Expression"}, {5, "test Expression"}, {6, "test Expression"}, {7, "test Expression"}},
			ResetData: true,
		},
		{
			TestName:  "UPDATE Expressions Invalid col",
			SQLStr:    "UPDATE testupdate SET col1 = colX+1, col2 = \"test \"+\"Expression\"",
			ExpErr:    "Error: Column \"colX\" not found in Table(s): testupdate",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{2, "test Expression"}, {3, "test Expression"}, {4, "test Expression"}, {5, "test Expression"}, {6, "test Expression"}, {7, "test Expression"}},
			ResetData: true,
		},
		{
			TestName:  "UPDATE Expressions Double Set Col",
			SQLStr:    "UPDATE testupdate SET col1 = col1+1, col2 = \"test \"+\"Expression\", col1=999",
			ExpErr:    "Syntax Error: col1 is set more than once",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{2, "test Expression"}, {3, "test Expression"}, {4, "test Expression"}, {5, "test Expression"}, {6, "test Expression"}, {7, "test Expression"}},
			ResetData: true,
		},
		{
			TestName:  "UPDATE Negate int",
			SQLStr:    "UPDATE testupdate SET col1 = -col1",
			ExpErr:    "",
			TableName: "testupdate",
			Cols:      []string{"col1", "col2"},
			ExpData:   sqtypes.RawVals{{-6, "test row 6"}, {-5, "test row 5"}, {-4, "test row 4"}, {-3, "test row 3"}, {-2, "test row 2"}, {-1, "test row 1"}},
			ResetData: true,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testUpdateFunc(row))

	}
}

func testUpdateFunc(d UpdateData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		if d.ResetData {
			err := resetUpdateData()
			if err != nil {
				t.Errorf("Reset Update Data failed: %s", err)
				return
			}
		}
		profile := sqprofile.CreateSQProfile()
		tkns := tokens.Tokenize(d.SQLStr)
		_, data, err := cmd.Update(profile, tkns)
		if data != nil {
			t.Errorf("Update returned a non nil dataset")
			return
		}
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if d.ExpData != nil {
			cList := sqtables.ColsToExpr(sqtables.NewColListNames(d.Cols))
			tab, err := sqtables.GetTable(profile, d.TableName)
			if err != nil {
				t.Error(err)
				return
			}

			ds, err := tab.GetRowData(profile, cList, nil, nil)
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

func resetUpdateData() error {
	profile := sqprofile.CreateSQProfile()

	str := "delete from testupdate"
	tkns := tokens.Tokenize(str)
	_, _, err := cmd.Delete(profile, tkns)
	if err != nil {
		return err
	}
	str = "INSERT INTO testupdate (col1, col2) VALUES " +
		"(1, \"test row 1\"), " +
		"(2, \"test row 2\"), " +
		"(3, \"test row 3\"), " +
		"(4, \"test row 4\"), " +
		"(5, \"test row 5\"), " +
		"(6, \"test row 6\")"
	tkns = tokens.Tokenize(str)
	_, _, err = cmd.InsertInto(profile, tkns)
	return err
}
