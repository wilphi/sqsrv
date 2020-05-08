package sqtables_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

type CreateTableData struct {
	TestName  string
	TableName string
	Cols      []column.Def
	Profile   *sqprofile.SQProfile
	ExpErr    string
}

func TestCreateTable(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	data := []CreateTableData{
		{
			TestName:  "CREATE TABLE Underscore",
			TableName: "_createtest1",
			Cols: []column.Def{
				column.NewDef("col1", tokens.Int, false),
				column.NewDef("col2", tokens.Bool, false),
			},
			Profile: profile,
			ExpErr:  "Error: Invalid Name: _createtest1 - Only system tables may begin with _",
		},
		{
			TestName:  "CREATE TABLE test1",
			TableName: "createtest1",
			Cols: []column.Def{
				column.NewDef("col1", tokens.Int, false),
				column.NewDef("col2", tokens.Bool, false),
			},
			Profile: profile,
			ExpErr:  "",
		},
		{
			TestName:  "CREATE TABLE Duplicate",
			TableName: "createtest1",
			Cols: []column.Def{
				column.NewDef("col1", tokens.Int, false),
				column.NewDef("col2", tokens.Bool, false),
			},
			Profile: profile,
			ExpErr:  "Error: Invalid Name: Table createtest1 already exists",
		},
		{
			TestName:  "CREATE TABLE Different case Duplicate",
			TableName: "CREATEtest1",
			Cols: []column.Def{
				column.NewDef("col1", tokens.Int, false),
				column.NewDef("col2", tokens.Bool, false),
			},
			Profile: profile,
			ExpErr:  "Error: Invalid Name: Table createtest1 already exists",
		},
		{
			TestName:  "CREATE TABLE No Cols",
			TableName: "createtest2",
			Cols:      []column.Def{},
			Profile:   profile,
			ExpErr:    "Error: Create Table: table must have at least one column",
		},
		{
			TestName:  "CREATE TABLE Not Null",
			TableName: "createtest2",
			Cols: []column.Def{
				column.NewDef("city", tokens.String, true),
				column.NewDef("street", tokens.String, false),
				column.NewDef("streetno", tokens.Int, false),
			},
			Profile: profile,
			ExpErr:  "",
		},
		{
			TestName:  "CREATE TABLE all Not Null",
			TableName: "createtest3",
			Cols: []column.Def{
				column.NewDef("city", tokens.String, true),
				column.NewDef("street", tokens.String, true),
				column.NewDef("streetno", tokens.Int, true),
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
		defer sqtest.PanicTestRecovery(t, "")

		originalList, err := sqtables.CatalogTables(d.Profile)
		if err != nil {
			t.Error(err)
			return
		}
		tab := sqtables.CreateTableDef(d.TableName, d.Cols...)
		err = sqtables.CreateTable(d.Profile, tab)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		finalList, err := sqtables.CatalogTables(d.Profile)
		if err != nil {
			t.Error(err)
			return
		}
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
	tableName := "droptest1"
	testT := sqtables.CreateTableDef(tableName,
		column.NewDef("rownum", tokens.Int, false),
		column.NewDef("col1", tokens.String, false),
		column.NewDef("col2", tokens.Int, false),
		column.NewDef("col3", tokens.Bool, false),
	)
	err := sqtables.CreateTable(profile, testT)
	if err != nil {
		t.Error("Error creating table: ", err)
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
		defer sqtest.PanicTestRecovery(t, "")

		originalList, err := sqtables.CatalogTables(d.Profile)
		if err != nil {
			t.Error(err)
			return
		}
		err = sqtables.DropTable(d.Profile, d.TableName)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		finalList, err := sqtables.CatalogTables(d.Profile)
		if err != nil {
			t.Error(err)
			return
		}
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

	originalList, err := sqtables.CatalogTables(profile)
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	originalAllList, err := sqtables.CatalogAllTables(profile)
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	tab := sqtables.CreateTableDef(
		"tablea",
		column.NewDef("col1", tokens.Int, false),
		column.NewDef("col2", tokens.String, false),
	)
	err = sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	tab2 := sqtables.CreateTableDef(
		"tableb",
		column.NewDef("col1", tokens.Int, false),
		column.NewDef("col2", tokens.String, false),
	)
	err = sqtables.CreateTable(profile, tab2)
	if err != nil {
		t.Error("Error setting up data for TestDropTable ", err)
		return
	}
	tabdrop := sqtables.CreateTableDef(
		"tabledrop",
		column.NewDef("col1", tokens.Int, false),
		column.NewDef("col2", tokens.String, false),
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
		defer sqtest.PanicTestRecovery(t, "")

		tList, err := sqtables.CatalogTables(profile)
		if err != nil {
			t.Error(err)
			return
		}
		if len(originalList)+2 != len(tList) {
			t.Error("Tables not added correctly to tables list")
			return
		}
	})

	t.Run("Tables All List", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tList, err := sqtables.CatalogAllTables(profile)
		if err != nil {
			t.Error(err)
			return
		}

		if len(originalAllList)+3 != len(tList) {
			t.Error("Tables not added correctly to tables list")
			return
		}
	})

	t.Run("Lock All Tables", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		sqtables.LockCatalog(profile)
	})
	t.Run("UnLock All Tables", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		sqtables.UnlockCatalog(profile)
	})
	t.Run("underscore test", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tab := sqtables.CreateTableDef("", column.NewDef("col1", tokens.String, false))
		err := sqtables.CreateTable(profile, tab)
		experr := "Error: Invalid Name: Table names can not be blank"
		if err.Error() != experr {
			t.Errorf("Expected error: %q, Actual Error: %q", experr, err)
		}
	})
}
