package cmd_test

import (
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

type DropData struct {
	TestName  string
	Command   string
	ExpErr    string
	TableName string
}

func testDropFunc(profile *sqprofile.SQProfile, d DropData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		tkns := tokens.Tokenize(d.Command)
		_, data, err := cmd.DropTable(profile, tkns)
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
			t.Error("Drop Table function should always return nil data")
			return
		}
		tab := sqtables.GetTable(profile, d.TableName)
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
	_, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Errorf("Error setting up table for %s: %s", t.Name(), err)
		return
	}

	// Test to see what happens with empty table
	tkns = tokens.Tokenize("CREATE TABLE dropEmpty (col1 int, col2 string, col3 bool)")
	_, err = cmd.CreateTableFromTokens(profile, tkns)
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
	if _, _, err := cmd.InsertInto(profile, tkns); err != nil {
		t.Fatalf("Unexpected Error setting up test for %s: %s", t.Name(), err.Error())
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
			TestName:  "Drop Table with Data Rows",
			Command:   "Drop TABLE droptest",
			TableName: "Droptest",
			ExpErr:    "",
		},
		{
			TestName:  "Drop Table with extra stuff",
			Command:   "Drop TABLE droptest extra stuff",
			TableName: "Droptest",
			ExpErr:    "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDropFunc(profile, row))

	}
}
