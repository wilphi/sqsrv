package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

func testNewDataSetFunc(
	tables sqtables.TableList,
	eList *sqtables.ExprList, groupBy *column.List, ExpErr string,
) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()

		data, err := sqtables.NewDataSet(profile, tables, eList)
		if sqtest.CheckErr(t, err, ExpErr) {
			return
		}

		colStr := eList.Names(false)
		c := data.GetColNames()
		if !reflect.DeepEqual(c, colStr) {
			t.Errorf("Column lists do not match: %v, %v", c, colStr)
			return
		}
		if data.Len() != 0 {
			t.Errorf("There should be no data in Dataset")

		}

		if data.NumCols() != eList.Len() {
			t.Error("NumCols does not match eList.Len")
		}
	}
}

func TestDataSet(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tableName := "dataset"
	tab := sqtables.CreateTableDef(tableName,
		[]column.Def{
			column.NewDef("col1", tokens.Int, false),
			column.NewDef("col2", tokens.String, false),
			column.NewDef("col3", tokens.Bool, false),
		},
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	vals := sqtypes.RawVals{
		{"ttt", 1},
		{"ttt", 9},
		{"aaa", 6},
		{"aaa", 6},
		{"qqq", 4},
		{"qab", 2},
		{"qxc", 8},
		{"nnn", 1},
	}.ValueMatrix()
	valsWithNull := sqtypes.RawVals{
		{nil, 11},
		{"ttt", 1},
		{"ttt", 9},
		{"aaa", 6},
		{"aaa", 6},
		{nil, 12},
		{"qqq", 4},
		{"qab", 2},
		{"qxc", 8},
		{"nnn", 1},
		{nil, 10},
	}.ValueMatrix()
	allNullVals := sqtypes.RawVals{
		{nil, nil},
		{nil, nil},
		{nil, 10},
	}.ValueMatrix()
	//colStr := []string{"col2", "col1"}
	colStrErr := []string{"col2", "col1", "colX"}

	colds := []column.Ref{{ColName: "col2", ColType: tokens.String}, {ColName: "col1", ColType: tokens.Int}}
	exprCols := sqtables.ColsToExpr(column.NewListRefs(colds))
	emptyExprCols := &sqtables.ExprList{}
	exprColsErr := sqtables.ColsToExpr(column.NewListNames(colStrErr))

	if tab == nil {
		t.Error("Unable to get table for testing")
		return
	}
	tables := sqtables.NewTableListFromTableDef(profile, tab)

	t.Run("New DataSet", testNewDataSetFunc(tables, exprCols, nil, ""))

	t.Run("New DataSet no Cols", testNewDataSetFunc(tables, emptyExprCols, nil, "Internal Error: Expression List is empty for new DataSet"))
	t.Run("New DataSet with Validate Err", testNewDataSetFunc(tables,
		exprColsErr,
		nil,
		"Error: Column \"colX\" not found in Table(s): dataset",
	))

	t.Run("Len==0 from DataSet", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		data, err := sqtables.NewDataSet(profile, tables, exprCols)
		if err != nil {
			t.Errorf("Unexpected Error in test: %s", err.Error())
			return
		}

		if data.Len() != 0 {
			t.Error("There should be no data")
		}
	})

	t.Run("GetTable from Dataset", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		data, err := sqtables.NewDataSet(profile, tables, exprCols)
		if err != nil {
			t.Errorf("Unexpected Error in test: %s", err.Error())
			return
		}

		if !tables.Equal(data.GetTables()) {
			t.Error("Tables do not match")
		}
	})
	t.Run("GetColList from Dataset", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		data, err := sqtables.NewDataSet(profile, tables, exprCols)
		if err != nil {
			t.Errorf("Unexpected Error in test: %s", err.Error())
			return
		}
		clist := data.GetColList().GetColNames()

		nlist := data.GetColNames()
		elist := exprCols.Names(false)
		//	fmt.Printf("clist = %v\n", clist)
		//	fmt.Printf("elist = %v\n", elist)
		if !reflect.DeepEqual(nlist, elist) {
			t.Error("Collist names do not match dataset")
		}
		if !reflect.DeepEqual(clist, elist) {
			t.Error("GetColList does not match expected col list")
		}
	})

	rw1 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"aaa", 6})
	rw2 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"nnn", 1})
	rw3 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"qab", 2})
	rw4 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"qqq", 4})
	rw5 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"qxc", 8})
	rw6 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"ttt", 1})
	rw7 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"ttt", 9})
	rwNil10 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{nil, 10})
	rwNil11 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{nil, 11})
	rwNil12 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{nil, 12})
	rwANill := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{nil, nil})
	data := []SortData{
		{
			TestName:     "Sort Dataset Invalid Order Col",
			Tables:       tables,
			DataCols:     exprCols,
			InitVals:     vals,
			Order:        sqtables.SortOrder{{ColName: "colX", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:      sqtypes.ValueMatrix{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
			SortOrderErr: "Error: Column colX not found in dataset",
		},
		{
			TestName:     "Sort Dataset skip Invalid Order Col",
			Tables:       tables,
			DataCols:     exprCols,
			InitVals:     vals,
			Order:        sqtables.SortOrder{{ColName: "colX", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:      sqtypes.ValueMatrix{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
			SortOrderErr: "Error: Column colX not found in dataset",
		},
		{
			TestName: "Sort Dataset",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    sqtables.SortOrder{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			SONames:  []string{"col2", "col1"},
			SOString: "(col2, col1)",
			ExpVals:  sqtypes.ValueMatrix{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
		},
		{
			TestName: "Sort Empty Dataset",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: sqtypes.ValueMatrix{},
			Order:    sqtables.SortOrder{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			SONames:  []string{"col2", "col1"},
			SOString: "(col2, col1)",
			ExpVals:  sqtypes.ValueMatrix{},
		},
		{
			TestName: "Sort Dataset desc",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    sqtables.SortOrder{{ColName: "col2", SortType: tokens.Desc}, {ColName: "col1", SortType: tokens.Desc}},
			SONames:  []string{"col2", "col1"},
			SOString: "(col2 DESC, col1 DESC)",
			ExpVals:  sqtypes.ValueMatrix{rw7, rw6, rw5, rw4, rw3, rw2, rw1, rw1},
		},
		{
			TestName: "Sort Dataset desc/asc",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    sqtables.SortOrder{{ColName: "col2", SortType: tokens.Desc}, {ColName: "col1", SortType: tokens.Asc}},
			SONames:  []string{"col2", "col1"},
			SOString: "(col2 DESC, col1)",
			ExpVals:  sqtypes.ValueMatrix{rw6, rw7, rw5, rw4, rw3, rw2, rw1, rw1},
		},
		{
			TestName: "Sort Dataset no order",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    nil,
			ExpVals:  sqtypes.ValueMatrix{rw6, rw7, rw5, rw4, rw3, rw2, rw1, rw1},
			SortErr:  "Error: Sort Order has not been set for DataSet",
		},
		{
			TestName: "Sort Dataset with Distinct",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    nil,
			ExpVals:  sqtypes.ValueMatrix{rw1, rw2, rw3, rw4, rw5, rw6, rw7},
			Distinct: true,
		},
		{
			TestName: "Sort Dataset with nulls",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: valsWithNull,
			SONames:  []string{"col2", "col1"},
			Order:    sqtables.SortOrder{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			SOString: "(col2, col1)",
			ExpVals:  sqtypes.ValueMatrix{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7, rwNil10, rwNil11, rwNil12},
		},
		{
			TestName: "Sort Dataset with all nulls",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: allNullVals,
			SONames:  []string{"col2", "col1"},
			Order:    sqtables.SortOrder{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			SOString: "(col2, col1)",
			ExpVals:  sqtypes.ValueMatrix{rwNil10, rwANill, rwANill},
		},
		{
			TestName: "Sort Dataset with nulls DESC",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: valsWithNull,
			Order:    sqtables.SortOrder{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Desc}},
			SONames:  []string{"col2", "col1"},
			SOString: "(col2, col1 DESC)",
			ExpVals: sqtypes.RawVals{
				{"aaa", 6}, {"aaa", 6}, {"nnn", 1}, {"qab", 2}, {"qqq", 4}, {"qxc", 8}, {"ttt", 9}, {"ttt", 1}, {nil, 12}, {nil, 11}, {nil, 10},
			}.ValueMatrix(),
		},
		{
			TestName: "Sort Dataset with Distinct & Nulls",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: valsWithNull,
			Order:    nil,
			ExpVals:  sqtypes.ValueMatrix{rw1, rw2, rw3, rw4, rw5, rw6, rw7, rwNil10, rwNil11, rwNil12},
			Distinct: true,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSortFunc(row))

	}

}

type SortData struct {
	TestName     string
	Tables       sqtables.TableList
	DataCols     *sqtables.ExprList
	InitVals     sqtypes.ValueMatrix
	Order        sqtables.SortOrder
	SONames      []string
	SOString     string
	ExpVals      sqtypes.ValueMatrix
	SortOrderErr string
	SortErr      string
	Distinct     bool
}

func testSortFunc(d SortData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		data, err := sqtables.NewDataSet(profile, d.Tables, d.DataCols)
		if err != nil {
			t.Errorf("Unexpected Error in test: %s", err.Error())
			return
		}

		data.Vals = d.InitVals.Clone()

		if d.Distinct {
			data.Distinct()
		}

		if !(d.Distinct && d.Order == nil) {
			err := data.SetOrder(d.Order)
			if sqtest.CheckErr(t, err, d.SortOrderErr) {
				return
			}

			err = data.Sort()
			if sqtest.CheckErr(t, err, d.SortErr) {
				return
			}

		}
		if d.ExpVals != nil {
			msg := sqtypes.Compare2DValue(data.Vals, d.ExpVals, "Actual", "Expect", false)
			if msg != "" {
				t.Error(msg)
				fmt.Println(data.Vals)
				fmt.Println(d.ExpVals)
				return
			}
		}

		if d.Order.String() != d.SOString {
			t.Errorf("SortOrder.String does not match expected: %s", d.Order.String())
			return
		}
		if d.Order != nil {
			if !reflect.DeepEqual(d.Order.Names(), d.SONames) {
				t.Errorf("SortOrder names dont match expected for Names() %s:", d.Order.Names())
				return
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////////////////////////
//

func TestDSColVal(t *testing.T) {

	DSRow1 := sqtables.DSRow{
		Ptr:       sqptr.SQPtr(1),
		Vals:      sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{1, "test", false}),
		TableName: "Does not matter",
	}

	data := []DSColVal{
		{
			TestName: "First Col",
			Row:      DSRow1,
			Col:      &column.Ref{ColName: "test0", ColType: tokens.Int, Idx: 0},
			ExpErr:   "",
			ExpVal:   sqtypes.Raw(1),
		},
		{
			TestName: "Error -1 idx",
			Row:      DSRow1,
			Col:      &column.Ref{ColName: "test1", ColType: tokens.Int, Idx: -1},
			ExpErr:   "Error: Invalid index (-1) for Column in row. Col len = 3",
			ExpVal:   sqtypes.Raw(1),
		},
		{
			TestName: "Error idx=len",
			Row:      DSRow1,
			Col:      &column.Ref{ColName: "testx", ColType: tokens.Int, Idx: len(DSRow1.Vals)},
			ExpErr:   "Error: Invalid index (3) for Column in row. Col len = 3",
			ExpVal:   sqtypes.Raw(1),
		},
		{
			TestName: "Error idx>len",
			Row:      DSRow1,
			Col:      &column.Ref{ColName: "testx", ColType: tokens.Int, Idx: len(DSRow1.Vals) + 1},
			ExpErr:   "Error: Invalid index (4) for Column in row. Col len = 3",
			ExpVal:   sqtypes.Raw(1),
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDSColValFunc(row))

	}

}

type DSColVal struct {
	TestName string
	Row      sqtables.DSRow
	Col      *column.Ref
	ExpErr   string
	ExpVal   sqtypes.Raw
}

func testDSColValFunc(d DSColVal) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		s := d.Row.GetTableName(profile)
		if s != sqtables.DataSetTableName {
			t.Errorf("GetTableName (%q) did not return expected value (%q)", s, sqtables.DataSetTableName)
		}
		if d.Row.GetPtr(profile) != d.Row.Ptr {
			t.Errorf("GetPtr (%d) did not return expected value (%d)", d.Row.GetPtr(profile), d.Row.Ptr)
		}

		v, err := d.Row.ColVal(profile, d.Col)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		expVal := sqtypes.RawValue(d.ExpVal)

		if !v.Equal(expVal) {
			t.Errorf("ColVal (%s) does not match expected value (%s)", v.String(), expVal.String())
			return
		}

		if d.Row.IsDeleted(profile) != false {
			t.Error("DSRow IsDeleted() must return false")
			return
		}
		actVals := d.Row.GetVals(profile)
		for i, val := range d.Row.Vals {
			if !actVals[i].Equal(val) {
				t.Error("GetVals does not match d.Row.Vals")
				return
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////////////////////////
//

func TestDSIdxVal(t *testing.T) {

	DSRow1 := sqtables.DSRow{
		Ptr:       sqptr.SQPtr(1),
		Vals:      sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{1, "test", false}),
		TableName: "Does not matter",
	}

	data := []DSIdxValData{
		{
			TestName: "First Col",
			Row:      DSRow1,
			Col:      0,
			ExpErr:   "",
			ExpVal:   sqtypes.Raw(1),
		},
		{
			TestName: "Error -1 idx",
			Row:      DSRow1,
			Col:      -1,
			ExpErr:   "Error: Invalid index (-1) for row. Data len = 3",
			ExpVal:   sqtypes.Raw(1),
		},
		{
			TestName: "Error idx=len",
			Row:      DSRow1,
			Col:      len(DSRow1.Vals),
			ExpErr:   "Error: Invalid index (3) for row. Data len = 3",
			ExpVal:   sqtypes.Raw(1),
		},
		{
			TestName: "Error idx>len",
			Row:      DSRow1,
			Col:      len(DSRow1.Vals) + 1,
			ExpErr:   "Error: Invalid index (4) for row. Data len = 3",
			ExpVal:   sqtypes.Raw(1),
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDSIdxValFunc(row))

	}

}

type DSIdxValData struct {
	TestName string
	Row      sqtables.DSRow
	Col      int
	ExpErr   string
	ExpVal   sqtypes.Raw
}

func testDSIdxValFunc(d DSIdxValData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		s := d.Row.GetTableName(profile)
		if s != sqtables.DataSetTableName {
			t.Errorf("GetTableName (%q) did not return expected value (%q)", s, sqtables.DataSetTableName)
		}
		if d.Row.GetPtr(profile) != d.Row.Ptr {
			t.Errorf("GetPtr (%d) did not return expected value (%d)", d.Row.GetPtr(profile), d.Row.Ptr)
		}

		v, err := d.Row.IdxVal(profile, d.Col)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		expVal := sqtypes.RawValue(d.ExpVal)

		if !v.Equal(expVal) {
			t.Errorf("ColVal (%s) does not match expected value (%s)", v.String(), expVal.String())
			return
		}
	}
}
