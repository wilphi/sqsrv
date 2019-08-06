package sqtables_test

import (
	"fmt"
	"log"
	"reflect"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

type CreateTableData struct {
	TestName  string
	TableName string
	Cols      []sqtables.ColDef
	Profile   *sqprofile.SQProfile
	ExpErr    string
}

func TestCreateTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	data := []CreateTableData{
		{
			TestName:  "CREATE TABLE Underscore",
			TableName: "_createtest1",
			Cols: []sqtables.ColDef{
				sqtables.CreateColDef("col1", tokens.TypeInt, false),
				sqtables.CreateColDef("col2", tokens.TypeBool, false),
			},
			Profile: profile,
			ExpErr:  "Error: Invalid Name: _createtest1 - Only system tables may begin with _",
		},
		{
			TestName:  "CREATE TABLE test1",
			TableName: "createtest1",
			Cols: []sqtables.ColDef{
				sqtables.CreateColDef("col1", tokens.TypeInt, false),
				sqtables.CreateColDef("col2", tokens.TypeBool, false),
			},
			Profile: profile,
			ExpErr:  "",
		},
		{
			TestName:  "CREATE TABLE Duplicate",
			TableName: "createtest1",
			Cols: []sqtables.ColDef{
				sqtables.CreateColDef("col1", tokens.TypeInt, false),
				sqtables.CreateColDef("col2", tokens.TypeBool, false),
			},
			Profile: profile,
			ExpErr:  "Error: Invalid Name: Table createtest1 already exists",
		},
		{
			TestName:  "CREATE TABLE Different case Duplicate",
			TableName: "CREATEtest1",
			Cols: []sqtables.ColDef{
				sqtables.CreateColDef("col1", tokens.TypeInt, false),
				sqtables.CreateColDef("col2", tokens.TypeBool, false),
			},
			Profile: profile,
			ExpErr:  "Error: Invalid Name: Table createtest1 already exists",
		},
		{
			TestName:  "CREATE TABLE No Cols",
			TableName: "createtest2",
			Cols:      []sqtables.ColDef{},
			Profile:   profile,
			ExpErr:    "Error: Create Table: table must have at least one column",
		},
		{
			TestName:  "CREATE TABLE Not Null",
			TableName: "createtest2",
			Cols: []sqtables.ColDef{
				sqtables.CreateColDef("city", tokens.TypeString, true),
				sqtables.CreateColDef("street", tokens.TypeString, false),
				sqtables.CreateColDef("streetno", tokens.TypeInt, false),
			},
			Profile: profile,
			ExpErr:  "",
		},
		{
			TestName:  "CREATE TABLE all Not Null",
			TableName: "createtest3",
			Cols: []sqtables.ColDef{
				sqtables.CreateColDef("city", tokens.TypeString, true),
				sqtables.CreateColDef("street", tokens.TypeString, true),
				sqtables.CreateColDef("streetno", tokens.TypeInt, true),
			},
			Profile: profile,
			ExpErr:  "",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testCreateTableFunc(row))

	}
}

func testCreateTableFunc(d CreateTableData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		originalList := sqtables.ListTables(d.Profile)
		tab := sqtables.CreateTableDef(d.TableName, d.Cols...)
		err := sqtables.CreateTable(d.Profile, tab)
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Error(fmt.Sprintf("Unexpected Error in test: %s", err.Error()))
				return
			}
			if d.ExpErr != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", d.ExpErr, err.Error()))
				return
			}
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", d.ExpErr))
			return
		}
		finalList := sqtables.ListTables(d.Profile)
		originalList = append(originalList, d.TableName)
		sort.Strings(originalList)
		if !reflect.DeepEqual(originalList, finalList) {
			t.Errorf("Table %s does not seem to be added to table list", d.TableName)
		}
	}
}

type DropTableData struct {
	TestName  string
	TableName string
	Profile   *sqprofile.SQProfile
	ExpErr    string
}

func TestDropTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	tkns := tokens.Tokenize("CREATE TABLE droptest1 (col1 string, col2 int, col3 bool)")
	_, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	data := []DropTableData{
		{
			TestName:  "Drop TABLE Underscore",
			TableName: "_droptest1",
			Profile:   profile,
			ExpErr:    "Error: Invalid Name: _droptest1 - Unable to drop system tables",
		},
		{
			TestName:  "Drop TABLE invalid table",
			TableName: "ZZTable",
			Profile:   profile,
			ExpErr:    "Error: Invalid Name: Table zztable does not exist",
		},
		{
			TestName:  "Drop TABLE",
			TableName: "droptest1",
			Profile:   profile,
			ExpErr:    "",
		},
		{
			TestName:  "Drop TABLE double drop",
			TableName: "droptest1",
			Profile:   profile,
			ExpErr:    "Error: Invalid Name: Table droptest1 does not exist",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDropTableFunc(row))

	}
}

func testDropTableFunc(d DropTableData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		originalList := sqtables.ListTables(d.Profile)
		err := sqtables.DropTable(d.Profile, d.TableName)
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Error(fmt.Sprintf("Unexpected Error in test: %s", err.Error()))
				return
			}
			if d.ExpErr != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", d.ExpErr, err.Error()))
				return
			}
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", d.ExpErr))
			return
		}
		finalList := sqtables.ListTables(d.Profile)
		finalList = append(finalList, d.TableName)
		sort.Strings(finalList)
		if !reflect.DeepEqual(originalList, finalList) {
			t.Errorf("Table %s was not dropped correctly from table list", d.TableName)
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

func TestMiscTableList(t *testing.T) {
	// Data Setup
	profile := sqprofile.CreateSQProfile()

	originalList := sqtables.ListTables(profile)
	originalAllList := sqtables.ListAllTables(profile)

	tab := sqtables.CreateTableDef(
		"tablea",
		sqtables.CreateColDef("col1", tokens.TypeInt, false),
		sqtables.CreateColDef("col2", tokens.TypeString, false),
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	tab2 := sqtables.CreateTableDef(
		"tableb",
		sqtables.CreateColDef("col1", tokens.TypeInt, false),
		sqtables.CreateColDef("col2", tokens.TypeString, false),
	)
	err = sqtables.CreateTable(profile, tab2)
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	tabdrop := sqtables.CreateTableDef(
		"tabledrop",
		sqtables.CreateColDef("col1", tokens.TypeInt, false),
		sqtables.CreateColDef("col2", tokens.TypeString, false),
	)
	err = sqtables.CreateTable(profile, tabdrop)
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	err = sqtables.DropTable(profile, "tabledrop")
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	// \Data Setup

	t.Run("Tables List", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		tList := sqtables.ListTables(profile)

		if len(originalList)+2 != len(tList) {
			t.Error("Tables not added correctly to tables list")
			return
		}
	})

	t.Run("Tables All List", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		tList := sqtables.ListAllTables(profile)

		if len(originalAllList)+3 != len(tList) {
			t.Error("Tables not added correctly to tables list")
			return
		}
	})

	t.Run("Lock All Tables", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		sqtables.LockAllTables(profile)
	})
	t.Run("UnLock All Tables", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		sqtables.UnlockAllTables(profile)
	})
	t.Run("underscore test", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		tab := sqtables.CreateTableDef("", sqtables.CreateColDef("col1", tokens.TypeString, false))
		err := sqtables.CreateTable(profile, tab)
		experr := "Error: Invalid Name: Table names can not be blank"
		if err.Error() != experr {
			t.Errorf("Expected error: %q, Actual Error: %q", experr, err)
		}
	})
}
