package sqtables_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

type AddTableData struct {
	TestName  string
	TL        *sqtables.TableList
	TableName string
	Alias     string
	Table     *sqtables.TableDef
	InitLen   int
	PostLen   int
	ExpErr    string
}

func testAddTableFunc(d AddTableData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if d.TL.Len() != d.InitLen {
			t.Errorf("Expected Len (%d) Pre Add does not match actual len (%d) of TableList", d.InitLen, d.TL.Len())
			return
		}
		profile := sqprofile.CreateSQProfile()
		ft := sqtables.TableRef{Name: moniker.New(d.TableName, d.Alias), Table: d.Table}
		err := d.TL.Add(profile, ft)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if d.TL.Len() != d.PostLen {
			t.Errorf("Expected Len (%d) Post Add does not match actual len (%d) of TableList", d.PostLen, d.TL.Len())
			return
		}
	}
}
func TestAddTable(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	var err error
	profile := sqprofile.CreateSQProfile()
	tdata := []struct {
		Name string
		cols []column.Def
		Tab  *sqtables.TableDef
	}{
		{Name: "tlist1", cols: []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("col2", tokens.String, false)}},
		{Name: "tlist2", cols: []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("col2", tokens.String, false)}},
		{Name: "tlist3", cols: []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("col2", tokens.String, false)}},
	}
	for i := range tdata {
		tdata[i].Tab, err = createTestTable(profile, tdata[i].Name, tdata[i].cols...)
		if err != nil {
			t.Errorf("Error setting up data for %s: %s", "tlist1", err)
			return
		}
	}

	tList := sqtables.NewTableList(profile, nil)

	data := []AddTableData{
		{
			TestName:  "Empty List",
			TL:        tList,
			TableName: tdata[0].Name,
			Alias:     "",
			Table:     tdata[0].Tab,
			InitLen:   0,
			PostLen:   1,
			ExpErr:    "",
		},
		{
			TestName:  "Dup Add to List",
			TL:        tList,
			TableName: tdata[0].Name,
			Alias:     "",
			Table:     tdata[0].Tab,
			InitLen:   1,
			PostLen:   1,
			ExpErr:    "Error: Duplicate table name/alias \"tlist1\"",
		},
		{
			TestName:  "Add to List - no table def",
			TL:        tList,
			TableName: tdata[1].Name,
			Alias:     "",
			Table:     nil,
			InitLen:   1,
			PostLen:   2,
			ExpErr:    "",
		},
		{
			TestName:  "Add to List - Invalid table",
			TL:        tList,
			TableName: "NotATable",
			Alias:     "",
			Table:     nil,
			InitLen:   2,
			PostLen:   2,
			ExpErr:    "Error: Table \"notatable\" does not exist",
		},
		{
			TestName:  "Add to List a New table",
			TL:        tList,
			TableName: tdata[2].Name,
			Alias:     "",
			Table:     tdata[2].Tab,
			InitLen:   2,
			PostLen:   3,
			ExpErr:    "",
		},
		{
			TestName:  "Add to List -Add same table with Alias",
			TL:        tList,
			TableName: tdata[2].Name,
			Alias:     "alias2",
			Table:     tdata[2].Tab,
			InitLen:   3,
			PostLen:   4,
			ExpErr:    "",
		}}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testAddTableFunc(row))

	}

	// with the data nicely setup already, will run some one-off tests
	t.Run("FindTableDef with Name", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tab := tList.FindTableDef(profile, tdata[0].Name)
		if tab != tdata[0].Tab {
			t.Errorf("Unable to find TableDef for %s", tdata[0].Name)
		}
	})
	t.Run("FindTableDef with Alias", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tab := tList.FindTableDef(profile, "alias2")
		if tab != tdata[2].Tab {
			t.Errorf("Unable to find TableDef for %s", "alias2")
		}
	})
	t.Run("FindTableDef invalid table", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tab := tList.FindTableDef(profile, "NotATable")
		if tab != nil {
			t.Errorf("Unexpected table found %s", tab.GetName(profile))
		}
	})

	t.Run("AllCols", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		cols := tList.AllCols(profile)
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].Idx < cols[j].Idx })
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].TableName.Show() < cols[j].TableName.Show() })
		expCols := []column.Ref{
			{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: moniker.New("tlist3", "alias2"), DisplayTableName: true},
			{ColName: "col2", ColType: tokens.String, Idx: 1, IsNotNull: false, TableName: moniker.New("tlist3", "alias2"), DisplayTableName: true},
			{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: moniker.New("tlist1", ""), DisplayTableName: true},
			{ColName: "col2", ColType: tokens.String, Idx: 1, IsNotNull: false, TableName: moniker.New("tlist1", ""), DisplayTableName: true},
			{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: moniker.New("tlist2", ""), DisplayTableName: true},
			{ColName: "col2", ColType: tokens.String, Idx: 1, IsNotNull: false, TableName: moniker.New("tlist2", ""), DisplayTableName: true},
			{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: moniker.New("tlist3", ""), DisplayTableName: true},
			{ColName: "col2", ColType: tokens.String, Idx: 1, IsNotNull: false, TableName: moniker.New("tlist3", ""), DisplayTableName: true},
		}
		if !reflect.DeepEqual(expCols, cols) {
			t.Errorf("Actual cols do not match expected\nActual:%v\nExpect:%v", cols, expCols)
			return
		}
	})
}
func createTestTable(profile *sqprofile.SQProfile, tableName string, cols ...column.Def) (*sqtables.TableDef, error) {

	tab := sqtables.CreateTableDef(tableName, cols...)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		return nil, err
	}

	return tab, nil
}

func TestFindColDef(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	var err error

	///////////////////////////////////////////////////////////////
	// Data Setup
	profile := sqprofile.CreateSQProfile()
	tList := sqtables.NewTableList(profile, nil)
	tdata := []struct {
		Name  string
		Alias string
		cols  []column.Def
		Tab   *sqtables.TableDef
	}{
		{Name: "cdlist1", Alias: "", cols: []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("col2", tokens.String, false)}},
		{Name: "cdlist2", Alias: "", cols: []column.Def{column.NewDef("col123", tokens.Int, false), column.NewDef("col2", tokens.String, false)}},
		{Name: "cdlist3", Alias: "alias2", cols: []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("col2", tokens.String, false)}},
	}
	for i := range tdata {
		tdata[i].Tab, err = createTestTable(profile, tdata[i].Name, tdata[i].cols...)
		if err != nil {
			t.Errorf("Error setting up data for %s: %s", "tlist1", err)
			return
		}
		ft := sqtables.TableRef{Name: moniker.New(tdata[i].Name, tdata[i].Alias), Table: tdata[i].Tab}
		err = tList.Add(profile, ft)
		if err != nil {
			t.Errorf("Error setting up data for %s: %s", tdata[i].Name, err)
			return
		}
		if tdata[i].Alias != "" {
			ft.Name.SetAlias("")
			err = tList.Add(profile, ft)
			if err != nil {
				t.Errorf("Error setting up data for %s: %s", tdata[i].Name, err)
				return
			}
		}
	}

	/////////////////////////////////////////////////////
	data := []FindColDefData{
		{
			TestName:   "No Alias Valid Col",
			TL:         tList,
			ColName:    "col123",
			TableAlias: "",
			ExpCol:     &column.Def{ColName: "col123", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "cdlist2"},
			ExpErr:     "",
		},
		{
			TestName:   "No Alias InValid Col",
			TL:         tList,
			ColName:    "colX",
			TableAlias: "",
			ExpCol:     &column.Def{ColName: "col123", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "cdlist2"},
			ExpErr:     "Error: Column \"colX\" not found in Table(s): cdlist1, cdlist2, cdlist3, cdlist3",
		},
		{
			TestName:   "No Alias Muliple Table Col",
			TL:         tList,
			ColName:    "col2",
			TableAlias: "",
			ExpCol:     &column.Def{ColName: "col123", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "cdlist2"},
			ExpErr:     "Error: Column \"col2\" found in multiple tables, add tablename to differentiate",
		},
		{
			TestName:   "Alias with Valid Col",
			TL:         tList,
			ColName:    "col1",
			TableAlias: "alias2",
			ExpCol:     &column.Def{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "cdlist3"},
			ExpErr:     "",
		},
		{
			TestName:   "tableName with Valid Col",
			TL:         tList,
			ColName:    "col1",
			TableAlias: "cdlist1",
			ExpCol:     &column.Def{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "cdlist1"},
			ExpErr:     "",
		},
		{
			TestName:   "Alias with InValid Col",
			TL:         tList,
			ColName:    "colX",
			TableAlias: "cdlist1",
			ExpCol:     &column.Def{ColName: "col123", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "cdlist2"},
			ExpErr:     "Error: Column \"colX\" not found in Table \"cdlist1\"",
		},
		{
			TestName:   "Invalid Alias",
			TL:         tList,
			ColName:    "col1",
			TableAlias: "NotATable",
			ExpCol:     &column.Def{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "cdlist3"},
			ExpErr:     "Error: Table NotATable not found in table list",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testFindColDefFunc(row))

	}

}

type FindColDefData struct {
	TestName   string
	TL         *sqtables.TableList
	ColName    string
	TableAlias string
	ExpCol     *column.Def
	ExpErr     string
}

func testFindColDefFunc(d FindColDefData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		cd, err := d.TL.FindDef(profile, d.ColName, d.TableAlias)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if !reflect.DeepEqual(cd, d.ExpCol) {
			t.Errorf("Actual column.Ref %v does not match expected column.Ref %v", cd, d.ExpCol)
		}
	}
}
