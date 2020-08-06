package cmd_test

import (
	"fmt"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("cmd_test.log")
}

type DropData struct {
	TestName    string
	Command     string
	ExpErr      string
	TableName   string
	ManualTrans bool
}

func testDropFunc(profile *sqprofile.SQProfile, d DropData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")
		tkns := tokens.Tokenize(d.Command)
		trans := sqtables.BeginTrans(profile, !d.ManualTrans)
		_, data, err := cmd.DropTable(trans, tkns)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}
		if data != nil {
			t.Error("Drop Table function should always return nil data")
			return
		}
		tab, err := sqtables.GetTable(profile, d.TableName)
		if err != nil {
			t.Error(err)
			return
		}

		if tab != nil {
			t.Errorf("Drop table did not drop %s", d.TableName)
			return
		}

	}
}
func TestDropTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	//make sure table exists for testing
	tkns := tokens.Tokenize("CREATE TABLE droptest (col1 int, col2 string, col3 bool)")
	trans := sqtables.BeginTrans(profile, true)
	_, _, err := cmd.CreateTable(trans, tkns)
	if err != nil {
		t.Errorf("Error setting up table for %s: %s", t.Name(), err)
		return
	}

	// Test to see what happens with empty table
	tkns = tokens.Tokenize("CREATE TABLE dropEmpty (col1 int, col2 string, col3 bool)")
	_, _, err = cmd.CreateTable(trans, tkns)
	if err != nil {
		t.Errorf("Error setting up table for %s: %s", t.Name(), err)
		return
	}

	testData := "INSERT INTO droptest (col1, col2, col3) VALUES " +
		fmt.Sprintf("(%d, %q, %t),", 123, "With Cols Test", true) +
		fmt.Sprintf("(%d, %q, %t),", 456, "Seltest 2", true) +
		fmt.Sprintf("(%d, %q, %t),", 789, "Seltest 3", false) +
		fmt.Sprintf("(%d, %q, %t),", 456, "Seltest 4", true) +
		fmt.Sprintf("(%d, %q, %t),", 987, "Seltest 5", false) +
		fmt.Sprintf("(%d, %q, %t)", 654, "Seltest 6", true)

	tkns = tokens.Tokenize(testData)
	trans = sqtables.BeginTrans(profile, true)
	if _, _, err := cmd.InsertInto(trans, tkns); err != nil {
		t.Errorf("Unexpected Error setting up test for %s: %s", t.Name(), err.Error())
		return
	}

	data := []DropData{
		{
			TestName:  "Drop Table only",
			Command:   "Drop TABLE",
			TableName: "",
			ExpErr:    "Syntax Error: Expecting name of table to Drop",
		},
		{
			TestName:  "Drop Table an Empty table",
			Command:   "Drop TABLE DropEmpty",
			TableName: "DropEmpty",
			ExpErr:    "",
		},
		{
			TestName:  "Drop Table invalid table",
			Command:   "Drop TABLE NotATable",
			TableName: "NotATable",
			ExpErr:    "Error: Invalid Name: Table notatable does not exist",
		},
		{
			TestName:    "Drop Table with manual transaction",
			Command:     "Drop TABLE droptest",
			TableName:   "Droptest",
			ExpErr:      "Error: DDL statements cannot be executed within a transaction",
			ManualTrans: true,
		},
		{
			TestName:  "Drop Table with extra stuff",
			Command:   "Drop TABLE droptest extra stuff",
			TableName: "Droptest",
			ExpErr:    "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
		},
		{
			TestName:  "Drop Table with Data Rows",
			Command:   "Drop TABLE droptest",
			TableName: "Droptest",
			ExpErr:    "",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDropFunc(profile, row))

	}
}
