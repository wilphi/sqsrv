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

func testCreateTableFunc(profile *sqprofile.SQProfile, d CreateTableData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tkns := tokens.Tokenize(d.Command)
		tname, data, err := cmd.CreateTable(profile, tkns)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}
		if data != nil {
			t.Error("Create Table function should always return nil data")
			return
		}
		if tname != d.ExpTableName {
			t.Errorf("TableName: %q was the expected return, but actual value is: %q", d.ExpTableName, tname)
		}

		tab, err := sqtables.GetTable(profile, tname)
		if err != nil {
			t.Errorf("Table %s was not found by using GetTable", tname)
			return
		}
		actStr := tab.String(profile)
		if actStr != d.ExpStr {
			t.Errorf("Created table did not match expected: \nActual: %s\nExpected: %s", actStr, d.ExpStr)
		}
	}
}

type CreateTableData struct {
	TestName     string
	Command      string
	ExpErr       string
	ExpTableName string
	ExpStr       string
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
			ExpStr:       "createtest\n--------------------------------------\n\t{col1, INT}\n\t{col2, STRING}\n\t{col3, BOOL}\n",
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
			ExpStr:       "testnotnull\n--------------------------------------\n\t{col1, INT NOT NULL}\n\t{col2, STRING}\n\t{col3, BOOL}\n",
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
		{
			TestName:     "CREATE TABLE with PRIMARY KEY",
			Command:      "CREATE TABLE createpk (col1 int not null, col2 string not null, col3 bool), PRIMARY KEY (col1, col2)",
			ExpErr:       "",
			ExpTableName: "createpk",
			ExpStr: "createpk\n--------------------------------------\n\t{col1, INT NOT NULL}\n\t{col2, STRING NOT NULL}\n\t" +
				"{col3, BOOL}\n--------------------------------------\n\tPRIMARY KEY (col1, col2)\n",
		},
		{
			TestName:     "CREATE TABLE with PRIMARY KEY null1",
			Command:      "CREATE TABLE createpk (col1 int null, col2 string not null, col3 bool), PRIMARY KEY (col1, col2)",
			ExpErr:       "Syntax Error: Column col1 must not allow NULLs for Primary Key",
			ExpTableName: "createpk",
		},
		{
			TestName:     "CREATE TABLE with PRIMARY KEY null2",
			Command:      "CREATE TABLE createpk (col1 int not null, col2 string null, col3 bool), PRIMARY KEY (col1, col2)",
			ExpErr:       "Syntax Error: Column col2 must not allow NULLs for Primary Key",
			ExpTableName: "createpk",
		},
		{
			TestName:     "CREATE TABLE with UNIQUE",
			Command:      "CREATE TABLE createunique (col1 int not null, col2 string not null, col3 bool), UNIQUE unicon (col2)",
			ExpErr:       "",
			ExpTableName: "createunique",
			ExpStr: "createunique\n--------------------------------------\n\t{col1, INT NOT NULL}" +
				"\n\t{col2, STRING NOT NULL}\n\t{col3, BOOL}\n--------------------------------------" +
				"\n\tUNIQUE (col2)\n",
		},
		{
			TestName:     "CREATE TABLE with UNIQUE No Name",
			Command:      "CREATE TABLE createunique (col1 int not null, col2 string not null, col3 bool), UNIQUE (col2)",
			ExpErr:       "Syntax Error: Missing a name for the Unique constraint",
			ExpTableName: "createunique",
		},
		{
			TestName: "CREATE TABLE with UNIQUE/PK",
			Command: "CREATE TABLE createuniquepk (col1 int not null, col2 string not null, col3 int not null)," +
				" UNIQUE unicon (col2, col3), PRIMARY KEY (col1, col2)",
			ExpErr:       "",
			ExpTableName: "createuniquepk",
			ExpStr: "createuniquepk\n--------------------------------------\n\t{col1, INT NOT NULL}" +
				"\n\t{col2, STRING NOT NULL}\n\t{col3, INT NOT NULL}\n" +
				"--------------------------------------\n\tPRIMARY KEY (col1, col2)\n\tUNIQUE (col2, col3)\n",
		},
		{
			TestName: "CREATE TABLE with UNIQUE/PK*2",
			Command: "CREATE TABLE createuniquepk (col1 int not null, col2 string not null, col3 int not null)," +
				" PRIMARY KEY (col1, col3), UNIQUE unicon (col2, col3), PRIMARY KEY (col1, col2)",
			ExpErr:       "Syntax Error: The table createuniquepk cannot have more than one Primary Key",
			ExpTableName: "createuniquepk",
		},
		{
			TestName: "CREATE TABLE with PK err in cols",
			Command: "CREATE TABLE createpkerr (col1 int not null, col2 string not null, col3 int not null)," +
				" PRIMARY KEY (col1, colX)",
			ExpErr:       "Error: Column colX not found in table createpkerr for Primary Key",
			ExpTableName: "createpkerr",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testCreateTableFunc(profile, row))

	}

}
