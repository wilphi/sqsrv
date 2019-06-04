package sq

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

func testformatFunc(d formatData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()

		retVal := format(d.Num)
		if retVal != d.ExpVal {
			t.Errorf("Actual Value (%s) does not match expected value (%s)", retVal, d.ExpVal)
			return
		}
	}
}

type formatData struct {
	TestName string
	Num      uint64
	ExpVal   string
}

func TestInsertInto(t *testing.T) {

	data := []formatData{
		{TestName: "Single Digit", Num: 7, ExpVal: "7"},
		{TestName: "just Less than 1000", Num: 999, ExpVal: "999"},
		{TestName: "Thousand", Num: 1000, ExpVal: "1,000"},
		{TestName: "Thousand and one", Num: 1001, ExpVal: "1,001"},
		{TestName: "Very Large Number", Num: 1234567890, ExpVal: "1,234,567,890"},
		{TestName: "Extra Large Number", Num: 12345678900987654321, ExpVal: "12,345,678,900,987,654,321"},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testformatFunc(row))

	}

}

func testGetCmdFunc(profile *sqprofile.SQProfile, d GetCmdData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		if d.SaveDir {
			dir, err := ioutil.TempDir("", "sqtestgetcmd")
			if err != nil {
				t.Errorf("%s: Unable to create tempdir for test", t.Name())
			}
			sqtables.SetDBDir(dir)
			defer os.RemoveAll(dir)
		}
		tkns := tokens.Tokenize(d.Command)
		cmd := GetCmdFunc(*tkns)
		if cmd == nil {
			if !d.NilFunc {
				t.Errorf("Command does not exist")
			}
			return
		}
		if d.NilFunc {
			t.Error("GetCmd returned a function when nil was expected")
			return
		}
		response, shutdowntype, err := cmd(profile, tkns)
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
		if shutdowntype != d.ExpShutDown {
			t.Errorf("Actual shutdown type %d does not match expected %d", shutdowntype, d.ExpShutDown)
			return
		}
		if !strings.Contains(response.Msg, d.ExpMsg) {
			t.Errorf("Response Msg (%s) does not contain %s", response.Msg, d.ExpMsg)
		}
	}
}

type GetCmdData struct {
	TestName    string
	Command     string
	NilFunc     bool
	ExpMsg      string
	ExpShutDown ShutdownType
	ExpErr      string
	SaveDir     bool
}

func TestGetCmdFunction(t *testing.T) {

	profile := sqprofile.CreateSQProfile()
	tkns := tokens.Tokenize("create table getcmdtest (col1 int, col2 string)")
	tableName, _, err := cmd.CreateTable(profile, tkns)
	if err != nil {
		t.Errorf("%s: Unable to create table for test", t.Name())
	}
	tkns = tokens.Tokenize("INSERT INTO " + tableName + " (col1, col2) VALUES (1,\"test\")")
	_, _, err = cmd.InsertInto(profile, tkns)
	if err != nil {
		t.Errorf("%s: Unable to add data to table for test", t.Name())
	}

	data := []GetCmdData{
		{
			TestName:    "Invalid Command",
			Command:     "",
			NilFunc:     true,
			ExpMsg:      "",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "SQL Command",
			Command:     "Drop Table notatable",
			NilFunc:     true,
			ExpMsg:      "",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "ShutdownForce",
			Command:     "shutdown force",
			NilFunc:     false,
			ExpMsg:      "Server is shutting down...",
			ExpShutDown: ShutdownForce,
			ExpErr:      "",
		},
		{
			TestName:    "Shutdown",
			Command:     "shutdown",
			NilFunc:     false,
			ExpMsg:      "Server is shutting down...",
			ExpShutDown: Shutdown,
			ExpErr:      "",
		},
		{
			TestName:    "Memory Stats",
			Command:     "stats mem",
			NilFunc:     false,
			ExpMsg:      "Number of GoRoutines",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Lock Stats",
			Command:     "stats lock",
			NilFunc:     false,
			ExpMsg:      "Write Lock Stats",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Help Stats",
			Command:     "stats help",
			NilFunc:     false,
			ExpMsg:      "stats: show various statistics about the server",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Stats only",
			Command:     "stats",
			NilFunc:     false,
			ExpMsg:      "Invalid stats command, try stats help for more information",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Garbage Collection",
			Command:     "gc",
			NilFunc:     false,
			ExpMsg:      "Garbage collection completed in",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Lock Table does not exist",
			Command:     "lock notatable",
			NilFunc:     false,
			ExpMsg:      "Table not found",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Lock without Table",
			Command:     "lock",
			NilFunc:     false,
			ExpMsg:      "Lock command must be followed by tablename",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Lock " + tableName,
			Command:     "lock " + tableName,
			NilFunc:     false,
			ExpMsg:      "Locking table " + tableName,
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Unlock Table does not exist",
			Command:     "unlock notatable",
			NilFunc:     false,
			ExpMsg:      "Table not found",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "UnLock without Table",
			Command:     "unlock",
			NilFunc:     false,
			ExpMsg:      "Unlock command must be followed by tablename",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "UnLock " + tableName,
			Command:     "unlock " + tableName,
			NilFunc:     false,
			ExpMsg:      "Unlocked table " + tableName,
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Show Tables",
			Command:     "show tables",
			NilFunc:     false,
			ExpMsg:      "Table List\n----------------------\n",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Show Connections",
			Command:     "show conn",
			NilFunc:     false,
			ExpMsg:      "Current Connections",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Show help",
			Command:     "show help",
			NilFunc:     false,
			ExpMsg:      "show: Displays information about the structure of the database",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Show only",
			Command:     "show",
			NilFunc:     false,
			ExpMsg:      "Invalid show command, try show help for more information",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "Help only",
			Command:     "help",
			NilFunc:     false,
			ExpMsg:      "SQSRV is an in-memory SQL server with persistance to disk",
			ExpShutDown: NoAction,
			ExpErr:      "",
		},
		{
			TestName:    "checkpoint success",
			Command:     "checkpoint",
			NilFunc:     false,
			ExpMsg:      "Checkpoint Successful",
			ExpShutDown: NoAction,
			ExpErr:      "",
			SaveDir:     true,
		},
	}
	for i, row := range data {

		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetCmdFunc(profile, row))

	}

}

func testGetDispatchFunc(d GetDispatchData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		tkns := tokens.Tokenize(d.Command)
		cmd := GetDispatchFunc(*tkns)
		if cmd == nil {
			if !d.NilFunc {
				t.Errorf("Command does not exist")
			}
			return
		}
		if d.NilFunc {
			t.Error("GetCmd returned a function when nil was expected")
			return
		}

	}
}

type GetDispatchData struct {
	TestName string
	Command  string
	NilFunc  bool
}

func TestGetDispatchFunction(t *testing.T) {

	data := []GetDispatchData{
		{
			TestName: "No Tokens",
			Command:  "",
			NilFunc:  true,
		},
		{
			TestName: "Invalid SQL",
			Command:  "shutdown force",
			NilFunc:  true,
		},
		{
			TestName: "SELECT",
			Command:  "SELECT * from test",
			NilFunc:  false,
		},
		{
			TestName: "SELECT only",
			Command:  "SELECT",
			NilFunc:  false,
		},
		{
			TestName: "INSERT INTO",
			Command:  "INSERT INTO test (col1, col2) values (1, \"test\")",
			NilFunc:  false,
		},
		{
			TestName: "DELETE",
			Command:  "DELETE",
			NilFunc:  false,
		},
		{
			TestName: "CREATE TABLE ",
			Command:  "CREATE TABLE TEST",
			NilFunc:  false,
		},
		{
			TestName: "DROP TABLE",
			Command:  "Drop",
			NilFunc:  false,
		},
		{
			TestName: "DROP only",
			Command:  "DROP",
			NilFunc:  false,
		},
		{
			TestName: "UPDATE",
			Command:  "UPDATE",
			NilFunc:  false,
		},
	}
	for i, row := range data {

		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetDispatchFunc(row))

	}

}
