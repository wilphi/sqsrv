package cmd_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	tk "github.com/wilphi/sqsrv/tokens"
)

func testCreateTableFunc(profile *sqprofile.SQProfile, tkns *tk.TokenList, errTxt string, tableName string) func(*testing.T) {
	return func(t *testing.T) {
		tname, err := cmd.CreateTableFromTokens(profile, tkns)
		if err != nil {
			log.Println(err.Error())
			if errTxt == "" {
				t.Error(fmt.Sprintf("Unexpected Error in test: %s", err.Error()))
				return
			}
			if errTxt != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", errTxt, err.Error()))
				return
			}
			return
		}
		if err == nil && errTxt != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", errTxt))
			return
		}
		if tname != tableName {
			t.Error(fmt.Sprintf("TableName: %q was the expected return, but actual value is: %q", tableName, tname))
		}
	}
}
func TestCreateTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	var testStruct = []struct {
		TestName     string
		Command      string
		ExpErr       string
		ExpTableName string
	}{
		{"CREATE TABLE missing rest", "CREATE TABLE", "Syntax Error: Expecting name of table to create", ""},
		{"CREATE TABLE createtest missing (", "CREATE TABLE createtest", "Syntax Error: Expecting ( after name of table", ""},
		{"CREATE TABLE missing col", "CREATE TABLE createtest (", "Syntax Error: Expecting name of column", ""},
		{"CREATE TABLE extra comma", "CREATE TABLE createtest (col1 int, col2 string, col3 bool, )", "Syntax Error: Unexpected \",\" before \")\"", "createtest"},
		{"CREATE TABLE missing comma", "CREATE TABLE createtest (col1 int, col2 string col3 bool )", "Syntax Error: Comma is required to separate column definitions", "createtest"},
		{"CREATE TABLE missing type", "CREATE TABLE createtest (col1 int, col2 string, col3  )", "Syntax Error: Expecting column type", "createtest"},
		{"CREATE TABLE missing cols", "CREATE TABLE createtest ( )", "Syntax Error: No columns defined for table", "createtest"},
		{"CREATE TABLE success", "CREATE TABLE createtest (col1 int, col2 string, col3 bool)", "", "createtest"},
		{"CREATE TABLE Not Null", "CREATE TABLE testnotnull (col1 int not null, col2 string, col3 bool null)", "", "testnotnull"},
		{"CREATE TABLE Not missing Null", "CREATE TABLE createnull (col1 int not, col2 string, col3 bool null)", "Syntax Error: Expecting a NULL after NOT in Column definition", "createnull"},
	}

	for i, row := range testStruct {
		tlist := tk.Tokenize(row.Command)
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testCreateTableFunc(profile, tlist, row.ExpErr, row.ExpTableName))

	}

}
