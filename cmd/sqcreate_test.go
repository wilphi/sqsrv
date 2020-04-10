package cmd_test

import (
	"fmt"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("cmd_test.log")
}

func testCreateTableFunc(profile *sqprofile.SQProfile, d CreateTableData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		tkns := tokens.Tokenize(d.Command)
		tname, data, err := cmd.CreateTable(profile, tkns)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}
		if data != nil {
			t.Error("Drop Table function should always return nil data")
			return
		}
		if tname != d.ExpTableName {
			t.Error(fmt.Sprintf("TableName: %q was the expected return, but actual value is: %q", d.ExpTableName, tname))
		}
	}
}

type CreateTableData struct {
	TestName     string
	Command      string
	ExpErr       string
	ExpTableName string
}

func TestCreateTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	data := []CreateTableData{
		{
			TestName:     "CREATE TABLE missing rest",
			Command:      "CREATE TABLE",
			ExpErr:       "Syntax Error: Expecting name of table to create",
			ExpTableName: "",
		},
		{
			TestName:     "CREATE TABLE createtest missing (",
			Command:      "CREATE TABLE createtest",
			ExpErr:       "Syntax Error: Expecting ( after name of table",
			ExpTableName: "",
		},
		{
			TestName:     "CREATE TABLE missing col",
			Command:      "CREATE TABLE createtest (",
			ExpErr:       "Syntax Error: Expecting name of column",
			ExpTableName: "",
		},
		{
			TestName:     "CREATE TABLE extra comma",
			Command:      "CREATE TABLE createtest (col1 int, col2 string, col3 bool, )",
			ExpErr:       "Syntax Error: Unexpected \",\" before \")\"",
			ExpTableName: "createtest",
		},
		{
			TestName:     "CREATE TABLE missing comma",
			Command:      "CREATE TABLE createtest (col1 int, col2 string col3 bool )",
			ExpErr:       "Syntax Error: Comma is required to separate columns",
			ExpTableName: "createtest"},
		{
			TestName:     "CREATE TABLE missing type",
			Command:      "CREATE TABLE createtest (col1 int, col2 string, col3  )",
			ExpErr:       "Syntax Error: Expecting column type",
			ExpTableName: "createtest",
		},
		{
			TestName:     "CREATE TABLE missing cols",
			Command:      "CREATE TABLE createtest ( )",
			ExpErr:       "Syntax Error: No columns defined for table",
			ExpTableName: "createtest",
		},
		{
			TestName:     "CREATE TABLE success",
			Command:      "CREATE TABLE createtest (col1 int, col2 string, col3 bool)",
			ExpErr:       "",
			ExpTableName: "createtest",
		},
		{
			TestName:     "CREATE TABLE success Duplicate",
			Command:      "CREATE TABLE createtest (col1 int, col2 string, col3 bool)",
			ExpErr:       "Error: Invalid Name: Table createtest already exists",
			ExpTableName: "createtest",
		},
		{
			TestName:     "CREATE TABLE Not Null",
			Command:      "CREATE TABLE testnotnull (col1 int not null, col2 string, col3 bool null)",
			ExpErr:       "",
			ExpTableName: "testnotnull",
		},
		{
			TestName:     "CREATE TABLE Not missing Null",
			Command:      "CREATE TABLE createnull (col1 int not, col2 string, col3 bool null)",
			ExpErr:       "Syntax Error: Expecting a NULL after NOT in Column definition",
			ExpTableName: "createnull",
		},
		{
			TestName:     "CREATE TABLE extra tokens",
			Command:      "CREATE TABLE createtest3 (col1 int, col2 string, col3 bool) extra stuff",
			ExpErr:       "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
			ExpTableName: "createtest",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testCreateTableFunc(profile, row))

	}

}
