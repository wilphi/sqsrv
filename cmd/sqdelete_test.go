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

type DeleteData struct {
	TestName  string
	Command   string
	TableName string
	ExpErr    string
	ExpVals   sqtypes.RawVals
	Data      *sqtables.DataSet
}

func testDeleteFunc(profile *sqprofile.SQProfile, d DeleteData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()

		//Reset Data
		if d.Data != nil {
			tab, err := sqtables.GetTable(profile, d.TableName)
			if err != nil {
				t.Error(err)
				return
			}

			ptrs, err := tab.GetRowPtrs(profile, nil, false)
			if err != nil {
				t.Errorf("Reset Data Error in test: %s", err.Error())
			}
			err = tab.DeleteRowsFromPtrs(profile, ptrs, sqtables.HardDelete)
			if err != nil {
				t.Errorf("Reset Data Error in test: %s", err.Error())
			}
			_, err = tab.AddRows(profile, d.Data)
			if err != nil {
				t.Errorf("Reset Data Error in test: %s", err.Error())
			}
		}
		tkns := tokens.Tokenize(d.Command)
		_, data, err := cmd.Delete(profile, tkns)
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
		if data != nil {
			t.Error("Delete function should always return nil data")
			return
		}
		if d.ExpVals != nil {
			expVals := sqtypes.CreateValuesFromRaw(d.ExpVals)
			tab, err := sqtables.GetTable(profile, d.TableName)
			if err != nil {
				t.Error(err)
				return
			}

			data, err := tab.GetRowData(profile, sqtables.ColsToExpr(tab.GetCols(profile)), nil, nil)
			if err != nil {
				t.Error("Unable to get data from table")
				return
			}
			if !reflect.DeepEqual(expVals, data.Vals) {
				t.Error("Expected Values and Actual values do not match")
				return
			}
		}
	}
}
func TestDelete(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Make sure datasets are by default in RowID order
	sqtables.RowOrder = true

	//make sure table exists for testing
	tkns := tokens.Tokenize("CREATE TABLE deltest (col1 int, col2 string, col3 bool)")
	_, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for TestSelect: %s", err)
		return
	}

	// Test to see what happens with empty table
	tkns = tokens.Tokenize("CREATE TABLE delEmpty (col1 int, col2 string, col3 bool)")
	_, err = cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for TestSelect: %s", err)
		return
	}

	testData := "INSERT INTO deltest (col1, col2, col3) VALUES " +
		fmt.Sprintf("(%d, %q, %t),", 123, "With Cols Test", true) +
		fmt.Sprintf("(%d, %q, %t),", 456, "Seltest 2", true) +
		fmt.Sprintf("(%d, %q, %t),", 789, "Seltest 3", false) +
		fmt.Sprintf("(%d, %q, %t),", 456, "Seltest 4", true) +
		fmt.Sprintf("(%d, %q, %t),", 987, "Seltest 5", false) +
		fmt.Sprintf("(%d, %q, %t)", 654, "Seltest 6", true)

	tkns = tokens.Tokenize(testData)
	if _, _, err := cmd.InsertInto(profile, tkns); err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	tab, err := sqtables.GetTable(profile, "deltest")
	if err != nil {
		t.Error(err)
		return
	}

	ds, err := tab.GetRowData(profile, sqtables.ColsToExpr(tab.GetCols(profile)), nil, nil)
	if err != nil {
		t.Errorf("Error setting up table for TestSelect: %s", err)
		return
	}

	data := []DeleteData{
		{
			TestName:  "Delete only",
			Command:   "Delete",
			TableName: "",
			ExpErr:    "Syntax Error: Expecting FROM",
			ExpVals:   nil,
			Data:      nil,
		},
		{
			TestName:  "Delete FROM only",
			Command:   "Delete FROM ",
			TableName: "",
			ExpErr:    "Syntax Error: Expecting table name in Delete statement",
			ExpVals:   nil,
			Data:      nil,
		},
		{
			TestName:  "Delete FROM Invalid Table",
			Command:   "Delete FROM NotATable",
			TableName: "delEmpty",
			ExpErr:    "Error: Table NotATable does not exist for delete statement",
			ExpVals:   sqtypes.RawVals{},
			Data:      nil,
		},
		{
			TestName:  "Delete FROM Empty Table",
			Command:   "Delete FROM delEmpty",
			TableName: "delEmpty",
			ExpErr:    "",
			ExpVals:   sqtypes.RawVals{},
			Data:      nil,
		},
		{
			TestName:  "Delete FROM table",
			Command:   "Delete FROM deltest",
			TableName: "deltest",
			ExpErr:    "",
			ExpVals:   sqtypes.RawVals{},
			Data:      ds,
		},
		{
			TestName:  "Delete FROM table where col1 = 456 ",
			Command:   "Delete FROM deltest where col1 = 456",
			TableName: "deltest",
			ExpErr:    "",
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{789, "Seltest 3", false},
				{987, "Seltest 5", false},
				{654, "Seltest 6", true},
			},
			Data: ds,
		},
		{
			TestName:  "Delete FROM table where invalid ",
			Command:   "Delete FROM deltest where col1 = \"invalid\"",
			TableName: "deltest",
			ExpErr:    "Error: Type Mismatch: invalid is not an Int",
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{789, "Seltest 3", false},
				{987, "Seltest 5", false},
				{654, "Seltest 6", true},
			},
			Data: ds,
		},
		{
			TestName:  "Delete FROM table where invalid 2",
			Command:   "Delete FROM deltest where \"invalid\" =",
			TableName: "deltest",
			ExpErr:    "Syntax Error: Unexpected end to expression",
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{789, "Seltest 3", false},
				{987, "Seltest 5", false},
				{654, "Seltest 6", true},
			},
			Data: ds,
		},
		{
			TestName:  "Delete FROM table with where Extra stuff ",
			Command:   "Delete FROM deltest where col1 = 456 extra stuff",
			TableName: "deltest",
			ExpErr:    "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{789, "Seltest 3", false},
				{987, "Seltest 5", false},
				{654, "Seltest 6", true},
			},
			Data: ds,
		},
		{
			TestName:  "Delete FROM table Extra stuff ",
			Command:   "Delete FROM deltest extra stuff",
			TableName: "deltest",
			ExpErr:    "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{789, "Seltest 3", false},
				{987, "Seltest 5", false},
				{654, "Seltest 6", true},
			},
			Data: ds,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDeleteFunc(profile, row))

	}
}

func TestDeleteFromTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	t.Run("Invalid Table Name", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()

		_, err := cmd.DeleteFromTable(profile, "NotATable", nil)
		if err.Error() != "Error: Table NotATable does not exist for Delete statement" {
			t.Error("Expected error not returned: ", err)
		}
	})
}
