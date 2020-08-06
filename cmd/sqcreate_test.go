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
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		tkns := tokens.Tokenize(d.Command)
		trans := sqtables.BeginTrans(profile, !d.ManualTrans)
		tname, data, err := cmd.CreateTable(trans, tkns)
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
	ExpPanic     string
	ManualTrans  bool
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
			TestName:     "CREATE TABLE manualTrans",
			Command:      "CREATE TABLE mantrans (col1 int, col2 string, col3 bool)",
			ExpErr:       "Error: DDL statements cannot be executed within a transaction",
			ExpTableName: "mantrans",
			ExpStr:       "mantrans\n--------------------------------------\n\t{col1, INT}\n\t{col2, STRING}\n\t{col3, BOOL}\n",
			ManualTrans:  true,
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
			TestName:     "CREATE TABLE with missing constraint",
			Command:      "CREATE TABLE createpk (col1 int not null, col2 string not null, col3 bool), ",
			ExpErr:       "Syntax Error: Expecting a constraint clause (Primary Key, Foreign, Index, Unique) after comma",
			ExpTableName: "createpk",
			ExpStr:       "",
		},
		{
			TestName:     "CREATE TABLE with missing constraint with junk",
			Command:      "CREATE TABLE createpk (col1 int not null, col2 string not null, col3 bool), names",
			ExpErr:       "Syntax Error: Unexpected tokens after comma - [IDENT=names]",
			ExpTableName: "createpk",
			ExpStr:       "",
		},
		{
			TestName:     "CREATE TABLE with PRIMARY missing KEY",
			Command:      "CREATE TABLE createpk (col1 int not null, col2 string not null, col3 bool), PRIMARY (col1, col2)",
			ExpErr:       "Syntax Error: Table constraint missing keyword KEY after PRIMARY",
			ExpTableName: "createpk",
			ExpStr:       "",
		},
		{
			TestName:     "CREATE TABLE with PRIMARY KEY missing (",
			Command:      "CREATE TABLE createpk (col1 int not null, col2 string not null, col3 bool), PRIMARY KEY col1, col2)",
			ExpErr:       "Syntax Error: Expecting ( after PRIMARY KEY",
			ExpTableName: "createpk",
			ExpStr:       "",
		},
		{
			TestName:     "CREATE TABLE with PRIMARY KEY missing cols",
			Command:      "CREATE TABLE createpk (col1 int not null, col2 string not null, col3 bool), PRIMARY KEY ()",
			ExpErr:       "Syntax Error: No columns defined for table",
			ExpTableName: "createpk",
			ExpStr:       "",
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
			TestName:     "CREATE TABLE with UNIQUE no (",
			Command:      "CREATE TABLE createunique (col1 int not null, col2 string not null, col3 bool), UNIQUE col2 col2)",
			ExpErr:       "Syntax Error: Expecting ( after name of constraint",
			ExpTableName: "createunique",
		},
		{
			TestName:     "CREATE TABLE with UNIQUE err in list",
			Command:      "CREATE TABLE createunique (col1 int not null, col2 string not null, col3 bool), UNIQUE col2 (col2 col1)",
			ExpErr:       "Syntax Error: Comma is required to separate columns",
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
		{
			TestName: "CREATE TABLE with FK no Key",
			Command: "CREATE TABLE createfk (col1 int not null, col2 string not null, col3 int not null)," +
				" FOREIGN (col1, colX)",
			ExpErr:       "Syntax Error: Missing keyword KEY after FOREIGN",
			ExpTableName: "createfk",
		},
		{
			TestName: "CREATE TABLE with FK no (",
			Command: "CREATE TABLE createfk (col1 int not null, col2 string not null, col3 int not null)," +
				" FOREIGN KEY col1, colX)",
			ExpErr:       "Syntax Error: Expecting ( after name of constraint",
			ExpTableName: "createfk",
		},
		{
			TestName: "CREATE TABLE with FK missing name",
			Command: "CREATE TABLE createfk (col1 int not null, col2 string not null, col3 int not null)," +
				" FOREIGN KEY (col1, colX)",
			ExpErr:       "Syntax Error: Missing a name for the Foreign Key constraint",
			ExpTableName: "createfk",
		},
		{
			TestName: "CREATE TABLE with FK err in cols",
			Command: "CREATE TABLE createfk (col1 int not null, col2 string not null, col3 int not null)," +
				" FOREIGN KEY cfk (col1 col2)",
			ExpErr:       "Syntax Error: Comma is required to separate columns",
			ExpTableName: "createfk",
		},
		{
			TestName: "CREATE TABLE with FK col not found",
			Command: "CREATE TABLE createfk (col1 int not null, col2 string not null, col3 int not null)," +
				" FOREIGN KEY cfk (col1, colX)",
			ExpErr:       "Syntax Error: Missing a name for the Foreign Key constraint",
			ExpTableName: "createfk",
			ExpPanic:     "Incomplete",
		},
		{
			TestName: "CREATE TABLE with Index",
			Command: "CREATE TABLE createidx (col1 int not null, col2 string not null, col3 int not null)," +
				" INDEX tabidx (col1, col2)",
			ExpErr:       "Syntax Error: Index Constraint not fully implemented",
			ExpTableName: "createidx",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testCreateTableFunc(profile, row))

	}

}
