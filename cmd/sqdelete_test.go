package cmd_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/wilphi/assertions"
	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqmutex"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqmutex.DefaultTimeout = time.Second
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
		defer sqtest.PanicTestRecovery(t, "")

		//Reset Data
		if d.Data != nil {
			tab, err := sqtables.GetTable(profile, d.TableName)
			assertions.AssertNoErr(err, "Unable to get table")

			ptrs, err := tab.GetRowPtrs(profile, nil, false)
			assertions.AssertNoErr(err, "Reset Data Error in test")

			err = tab.HardDeleteRowsFromPtrs(profile, ptrs)
			assertions.AssertNoErr(err, "Reset Data Error in test")

			_, err = tab.AddRows(sqtables.BeginTrans(profile, true), d.Data)
			assertions.AssertNoErr(err, "Reset Data Error in test")
		}
		tkns := tokens.Tokenize(d.Command)
		_, data, err := cmd.Delete(sqtables.BeginTrans(profile, true), tkns)
		if sqtest.CheckErr(t, err, d.ExpErr) {
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
			q := sqtables.Query{
				Tables: sqtables.NewTableListFromTableDef(profile, tab),
				EList:  sqtables.ColsToExpr(tab.GetCols(profile)),
			}
			data, err := q.GetRowData(profile)
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

	_, _, err := cmd.CreateTable(sqtables.BeginTrans(profile, true), tkns)
	assertions.AssertNoErr(err, "Error setting up table for TestDelete")

	// Test to see what happens with empty table
	tkns = tokens.Tokenize("CREATE TABLE delEmpty (col1 int, col2 string, col3 bool)")
	_, _, err = cmd.CreateTable(sqtables.BeginTrans(profile, true), tkns)
	assertions.AssertNoErr(err, "Error setting up table for TestDelete")

	testData := "INSERT INTO deltest (col1, col2, col3) VALUES " +
		fmt.Sprintf("(%d, %q, %t),", 123, "With Cols Test", true) +
		fmt.Sprintf("(%d, %q, %t),", 456, "Seltest 2", true) +
		fmt.Sprintf("(%d, %q, %t),", 789, "Seltest 3", false) +
		fmt.Sprintf("(%d, %q, %t),", 456, "Seltest 4", true) +
		fmt.Sprintf("(%d, %q, %t),", 987, "Seltest 5", false) +
		fmt.Sprintf("(%d, %q, %t)", 654, "Seltest 6", true)

	tkns = tokens.Tokenize(testData)
	_, _, err = cmd.InsertInto(sqtables.BeginTrans(profile, true), tkns)
	assertions.AssertNoErr(err, "Unexpected Error setting up test")

	tab, err := sqtables.GetTable(profile, "deltest")
	assertions.AssertNoErr(err, "Unable to find table")

	q := sqtables.Query{
		Tables: sqtables.NewTableListFromTableDef(profile, tab),
		EList:  sqtables.ColsToExpr(tab.GetCols(profile)),
	}
	ds, err := q.GetRowData(profile)
	assertions.AssertNoErr(err, "Error setting up table for TestDelete: %s")

	data := []DeleteData{
		{
			TestName:  "No Delete",
			Command:   "FROM deltest",
			TableName: "deltest",
			ExpErr:    "Syntax Error: Expecting DELETE",
			ExpVals:   sqtypes.RawVals{},
			Data:      ds,
		},
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
		{
			TestName:  "Delete FROM table where invalid col ",
			Command:   "Delete FROM deltest where colX = 456",
			TableName: "deltest",
			ExpErr:    "Error: Column \"colX\" not found in Table(s): deltest",
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
