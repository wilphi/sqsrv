package sqtables_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/sq"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
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
		ft := sqtables.TableRef{TableName: d.TableName, Alias: d.Alias, Table: d.Table}
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
			ExpErr:    "Error: Table \"NotATable\" does not exist",
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
		sort.SliceStable(cols, func(i, j int) bool { return cols[i].TableName < cols[j].TableName })
		expCols := []column.Ref{
			{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "tlist1", DisplayTableName: true},
			{ColName: "col2", ColType: tokens.String, Idx: 1, IsNotNull: false, TableName: "tlist1", DisplayTableName: true},
			{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "tlist2", DisplayTableName: true},
			{ColName: "col2", ColType: tokens.String, Idx: 1, IsNotNull: false, TableName: "tlist2", DisplayTableName: true},
			{ColName: "col1", ColType: tokens.Int, Idx: 0, IsNotNull: false, TableName: "tlist3", DisplayTableName: true},
			{ColName: "col2", ColType: tokens.String, Idx: 1, IsNotNull: false, TableName: "tlist3", DisplayTableName: true},
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
		ft := sqtables.TableRef{TableName: tdata[i].Name, Alias: tdata[i].Alias, Table: tdata[i].Tab}
		err = tList.Add(profile, ft)
		if err != nil {
			t.Errorf("Error setting up data for %s: %s", tdata[i].Name, err)
			return
		}
		if tdata[i].Alias != "" {
			ft.Alias = ""
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

////////////////////////////////////////////////////////////////
func TestTLGetRowData(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	//var err error
	profile := sqprofile.CreateSQProfile()
	sq.ProcessSQFile("./testdata/multitable.sq")
	tList := sqtables.NewTableList(profile,
		[]sqtables.TableRef{
			{TableName: "Person"},
			{TableName: "city"},
			{TableName: "country"},
		})

	eList := sqtables.ColsToExpr(
		column.NewListNames(
			[]string{
				"firstname",
				"lastname",
				"city.name",
				"city.prov",
				"country.short",
			},
		),
	)

	whereExpr := sqtables.NewOpExpr(
		sqtables.NewOpExpr(
			sqtables.NewOpExpr(
				sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.String, TableName: "city"}),
				tokens.Equal,
				sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: "country"}),
			),
			tokens.And,
			sqtables.NewOpExpr(
				sqtables.NewColExpr(column.Ref{ColName: "short", ColType: tokens.String, TableName: "country"}),
				tokens.NotEqual,
				sqtables.NewValueExpr(sqtypes.NewSQString("USA")),
			),
		),
		tokens.And,
		sqtables.NewOpExpr(
			sqtables.NewColExpr(column.Ref{ColName: "cityid", ColType: tokens.Int, TableName: "city"}),
			tokens.Equal,
			sqtables.NewColExpr(column.Ref{ColName: "cityid", ColType: tokens.Int, TableName: "person"}),
		),
	)

	data := []TLGetRowData{

		{
			TestName:  "Multitable Query",
			TL:        tList,
			ExprList:  eList,
			WhereExpr: whereExpr,
			ExpErr:    "",
			ExpVals: sqtypes.RawVals{
				{"Cornell", "Codilla", "Leeds", "Leeds", "GBR"},
				{"Georgia", "Kuffa", "Leeds", "Leeds", "GBR"},
				{"Sophie", "Schuh", "Leeds", "Leeds", "GBR"},
				{"Jenna", "Merisier", "Leeds", "Leeds", "GBR"},
				{"Ocie", "Capossela", "Hove", "Brighton and Hove", "GBR"},
				{"Linda", "Calco", "Hove", "Brighton and Hove", "GBR"},
				{"Svetlana", "Poirrier", "Sheffield", "Sheffield", "GBR"},
				{"Rodrigo", "Higman", "Manchester", "Manchester", "GBR"},
				{"Shelton", "Leggat", "Manchester", "Manchester", "GBR"},
				{"Grisel", "Martindale", "Joliette", "Québec", "CAN"},
				{"Elva", "Velten", "Joliette", "Québec", "CAN"},
				{"Nedra", "Hanaway", "Joliette", "Québec", "CAN"},
				{"Daron", "Whitcome", "Joliette", "Québec", "CAN"},
				{"Yvone", "June", "Joliette", "Québec", "CAN"},
				{"Tyrone", "Ringen", "Tofino", "British Columbia", "CAN"},
				{"Eliana", "Peasel", "Tofino", "British Columbia", "CAN"},
			},
		},
		{
			TestName:  "Nil Expression List",
			TL:        tList,
			ExprList:  nil,
			WhereExpr: whereExpr,
			ExpErr:    "Internal Error: Expression List must have at least one item",
		},
		{
			TestName:  "Empty Expression List",
			TL:        tList,
			ExprList:  sqtables.NewExprList(),
			WhereExpr: whereExpr,
			ExpErr:    "Internal Error: Expression List must have at least one item",
		},
		{
			TestName:  "Invalid colName in Expression List",
			TL:        tList,
			ExprList:  sqtables.NewExprList(sqtables.NewColExpr(column.Ref{ColName: "colX"})),
			WhereExpr: whereExpr,
			ExpErr:    "Error: Column \"colX\" not found in Table(s): Person, city, country",
		},
		{
			TestName:  "Invalid tablename in Expression List",
			TL:        tList,
			ExprList:  sqtables.NewExprList(sqtables.NewColExpr(column.Ref{ColName: "name", TableName: "NotATable"})),
			WhereExpr: whereExpr,
			ExpErr:    "Error: Table NotATable not found in table list",
		},
		{
			TestName:  "Empty Table List",
			TL:        sqtables.NewTableList(profile, nil),
			ExprList:  eList,
			WhereExpr: whereExpr,
			ExpErr:    "Internal Error: TableList must not be empty in TableList.GetRowData",
		},
		{
			TestName:  "Multitable Query No Where clause",
			TL:        tList,
			ExprList:  eList,
			WhereExpr: nil,
			ExpErr:    "Error: Multi table queries must have a valid where clause",
		},
		{
			TestName:  "Multitable Query err in Where clause",
			TL:        tList,
			ExprList:  eList,
			WhereExpr: sqtables.NewColExpr(column.Ref{ColName: "colX"}),
			ExpErr:    "Error: Column \"colX\" not found in Table(s): Person, city, country",
		},
		{
			TestName:  "Multitable Query Count()",
			TL:        tList,
			ExprList:  sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
			WhereExpr: whereExpr,
			ExpErr:    "",
			ExpVals: sqtypes.RawVals{
				{16},
			},
		},
		{
			TestName: "Multitable Query Cross Join Count()",
			TL:       tList,
			ExprList: sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: "person"}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Ava")),
					),
					tokens.Or,
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: "person"}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Luna")),
					),
				),
				tokens.And,
				sqtables.NewOpExpr(
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: "city"}),
					tokens.Equal,
					sqtables.NewValueExpr(sqtypes.NewSQString("Springfield")),
				),
			),
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{12},
			},
		},
		{
			TestName: "Multitable Query Cross Join with Cols",
			TL:       tList,
			ExprList: sqtables.NewExprList(
				sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: "person"}),
				sqtables.NewColExpr(column.Ref{ColName: "lastname", ColType: tokens.String, TableName: "person"}),
				sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: "city"}),
				sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.String, TableName: "city"}),
				sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: "country"}),
			),
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: "person"}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Ava")),
					),
					tokens.Or,
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: "person"}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Luna")),
					),
				),
				tokens.And,
				sqtables.NewOpExpr(
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: "city"}),
					tokens.Equal,
					sqtables.NewValueExpr(sqtypes.NewSQString("Springfield")),
				),
			),
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{"Ava", "Beilfuss", "Springfield", "United States", "Canada"},
				{"Ava", "Beilfuss", "Springfield", "United States", "Canada"},
				{"Ava", "Beilfuss", "Springfield", "United States", "United Kingdom"},
				{"Ava", "Beilfuss", "Springfield", "United States", "United Kingdom"},
				{"Ava", "Beilfuss", "Springfield", "United States", "United States"},
				{"Ava", "Beilfuss", "Springfield", "United States", "United States"},
				{"Luna", "Swantak", "Springfield", "United States", "Canada"},
				{"Luna", "Swantak", "Springfield", "United States", "Canada"},
				{"Luna", "Swantak", "Springfield", "United States", "United Kingdom"},
				{"Luna", "Swantak", "Springfield", "United States", "United Kingdom"},
				{"Luna", "Swantak", "Springfield", "United States", "United States"},
				{"Luna", "Swantak", "Springfield", "United States", "United States"},
			},
		},
		{
			TestName:  "Single table Query",
			TL:        sqtables.NewTableList(profile, []sqtables.TableRef{{TableName: "country"}}),
			ExprList:  sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("short", tokens.String, false))),
			WhereExpr: nil,
			ExpErr:    "",
			ExpVals: sqtypes.RawVals{
				{"GBR"},
				{"USA"},
				{"CAN"},
			},
		},
		{
			TestName:  "Single table Count() Query",
			TL:        sqtables.NewTableList(profile, []sqtables.TableRef{{TableName: "country"}}),
			ExprList:  sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
			WhereExpr: nil,
			ExpErr:    "",
			ExpVals: sqtypes.RawVals{
				{3},
			},
		},
		{
			TestName:  "Single table Count() Query group by country",
			TL:        sqtables.NewTableList(profile, []sqtables.TableRef{{TableName: "city"}}),
			ExprList:  sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("country", tokens.String, false)), sqtables.NewFuncExpr(tokens.Count, nil)),
			WhereExpr: nil,
			GroupBy:   sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("country", tokens.NilToken, false))),
			ExpErr:    "",
			ExpVals: sqtypes.RawVals{
				{"Canada", 2},
				{"United Kingdom", 4},
				{"United States", 48},
			},
		},

		{
			TestName: "Multitable Query Cross Join with Cols Group By firstname",
			TL:       tList,
			ExprList: sqtables.NewExprList(
				sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: "person"}),
				sqtables.NewFuncExpr(tokens.Count, nil),
			),
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: "person"}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Ava")),
					),
					tokens.Or,
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: "person"}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Luna")),
					),
				),
				tokens.And,
				sqtables.NewOpExpr(
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: "city"}),
					tokens.Equal,
					sqtables.NewValueExpr(sqtypes.NewSQString("Springfield")),
				),
			),
			GroupBy: sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("firstname", tokens.NilToken, false))),
			ExpErr:  "",
			ExpVals: sqtypes.RawVals{
				{"Ava", 6},
				{"Luna", 6},
			},
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testTLGetRowDataFunc(row))

	}

}

type TLGetRowData struct {
	TestName  string
	TL        *sqtables.TableList
	ExprList  *sqtables.ExprList
	WhereExpr sqtables.Expr
	ExpErr    string
	ExpVals   sqtypes.RawVals
	GroupBy   *sqtables.ExprList
}

func testTLGetRowDataFunc(d TLGetRowData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		data, err := d.TL.GetRowData(profile, d.ExprList, d.WhereExpr, d.GroupBy, nil)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		expVals := sqtypes.CreateValuesFromRaw(d.ExpVals)
		for x := range data.Vals[0] {
			sort.SliceStable(data.Vals, func(i, j int) bool { return data.Vals[i][x].LessThan(data.Vals[j][x]) })
			sort.SliceStable(expVals, func(i, j int) bool { return expVals[i][x].LessThan(expVals[j][x]) })
		}
		if !reflect.DeepEqual(data.Vals, expVals) {
			t.Errorf("Actual data does not match expected\nActual:%v\nExpect:%v", data.Vals, expVals)
			return
		}
	}
}
