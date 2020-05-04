package sqtables_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

const (
	withErr    = true
	withoutErr = false
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

type RowDataTest struct {
	TestName string
	Tab      *sqtables.TableDef
	Cols     *sqtables.ExprList
	Where    sqtables.Expr
	GroupBy  *sqtables.ExprList
	ExpErr   string
	ExpPtrs  []int
}

func testGetRowDataFunc(profile *sqprofile.SQProfile, d *RowDataTest) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tables := sqtables.NewTableListFromTableDef(profile, d.Tab)
		if d.Where != nil {
			err := d.Where.ValidateCols(profile, tables)
			if err != nil {
				t.Errorf("Unable to validate cols in Where %q", d.Where)
				return
			}
		}

		data, err := d.Tab.GetRowData(profile, d.Cols, d.Where, d.GroupBy, nil)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if len(d.ExpPtrs) != data.Len() {
			t.Errorf("The number of rows returned (%d) does not match expected rows (%d)", data.Len(), len(d.ExpPtrs))
			return
		}

		// make sure the row numbers match
		for i := range data.Vals {
			if !data.Vals[i][0].Equal(sqtypes.NewSQInt(d.ExpPtrs[i])) {
				t.Errorf("Returned Row num (%d) does not match expected (%d)", data.Vals[i][0], d.ExpPtrs[i])
			}
		}
	}
}

func TestGetRowData(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Data Setup
	tableName := "rowdatatest"
	testT := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("rownum", tokens.Int, false),
		sqtables.NewColDef("col1", tokens.Int, false),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.Bool, false),
	)
	err := sqtables.CreateTable(profile, testT)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}
	tables := sqtables.NewTableListFromTableDef(profile, testT)
	cols := sqtables.ColsToExpr(testT.GetCols(profile))
	dsData, err := sqtables.NewDataSet(profile, tables, cols)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, 5, "d test string", 10, true},
		{2, 7, "f test string", 100, false},
		{3, 17, "zz test string", 700, true},
	})
	_, err = testT.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	// Delete a row to make sure that soft deleted rows do not cause a problem
	where := sqtables.NewOpExpr(
		sqtables.NewColExpr(
			sqtables.NewColDef("rownum", tokens.Int, false)),
		tokens.Equal,
		sqtables.NewValueExpr(sqtypes.NewSQInt(3)),
	)
	err = where.ValidateCols(profile, tables)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	_, err = testT.DeleteRows(profile, where)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	testData := []RowDataTest{
		{
			TestName: "col1(5) = 5 ->1",
			Tab:      testT,
			Cols:     cols,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr:  "",
			ExpPtrs: []int{1},
		},
		{
			TestName: "col1(6) = 5 ->0",
			Tab:      testT,
			Cols:     cols,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(6)),
			),
			ExpErr:  "",
			ExpPtrs: []int{},
		},
		{
			TestName: "col1 < 5 ->0",
			Tab:      testT, Cols: cols,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.LessThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr:  "",
			ExpPtrs: []int{},
		},
		{
			TestName: "col1 < 7 ->1",
			Tab:      testT,
			Cols:     cols,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.LessThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(6)),
			),
			ExpErr:  "",
			ExpPtrs: []int{1},
		},
		{
			TestName: "Where Error",
			Tab:      testT,
			Cols:     cols,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col2", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(6)),
			),
			ExpErr:  "Error: Type Mismatch: 6 is not a String",
			ExpPtrs: []int{1},
		},
		/*		{
				TestName: "Count Expression",
				Tab:      testT,
				Cols:     sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
				WhereStr: "",
				ExpErr:   "",
				ExpPtrs:  []int{}, //does not return a pointer only the count of rows
			},*/
		{
			TestName: "Invalid Col in Expression",
			Tab:      testT,
			Cols: sqtables.NewExprList(
				sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
				sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.String, false)),
			),
			Where:   nil,
			ExpErr:  "Error: Column \"colX\" not found in Table(s): rowdatatest",
			ExpPtrs: []int{2},
		},
		{
			TestName: "Invalid function in Expression on Evaluate",
			Tab:      testT,
			Cols: sqtables.NewExprList(
				sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
				sqtables.NewFuncExpr(
					tokens.Float,
					sqtables.NewColExpr(sqtables.NewColDef("col2", tokens.String, false)),
				),
			),
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col2", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQString("d test string")),
			),
			ExpErr:  "strconv.ParseFloat: parsing \"d test string\": invalid syntax",
			ExpPtrs: []int{2},
		},
	}

	for i, row := range testData {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName), testGetRowDataFunc(profile, &row))
	}
}

type RowPtrsTest struct {
	TestName string
	Tab      *sqtables.TableDef
	Where    sqtables.Expr
	ExpErr   string
	ExpPtrs  sqptr.SQPtrs
	Sort     bool
}

func testGetRowPtrsFunc(profile *sqprofile.SQProfile, d *RowPtrsTest) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		var err error
		tables := sqtables.NewTableListFromTableDef(profile, d.Tab)

		if d.Where != nil {
			err = d.Where.ValidateCols(profile, tables)
			if err != nil {
				t.Errorf("Unable to validate cols in Where %q", d.Where)
				return
			}

		}
		ptrs, err := d.Tab.GetRowPtrs(profile, d.Where, d.Sort)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if len(d.ExpPtrs) != len(ptrs) {
			t.Errorf("The number of rows returned (%d) does not match expected rows (%d)", len(ptrs), len(d.ExpPtrs))
			return
		}

		// make sure the row numbers match
		sort.Slice(d.ExpPtrs, func(i, j int) bool { return ptrs[i] < ptrs[j] })
		for i := range ptrs {
			if ptrs[i] != d.ExpPtrs[i] {
				t.Errorf("Returned Row num (%d) does not match expected (%d)", ptrs[i], d.ExpPtrs[i])
			}
		}
	}
}

func TestGetRowPtrs(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	// Data Setup
	tableName := "rowptrstest"
	testT := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("rowid", tokens.Int, false),
		sqtables.NewColDef("firstname", tokens.String, false),
		sqtables.NewColDef("active", tokens.Bool, false),
	)
	err := sqtables.CreateTable(profile, testT)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}
	tables := sqtables.NewTableListFromTableDef(profile, testT)
	cols := sqtables.ColsToExpr(testT.GetCols(profile))
	dsData, err := sqtables.NewDataSet(profile, tables, cols)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, "Tim", true},
		{2, "Ted", true},
		{3, "Tex", true},
		{4, "Tad", true},
		{5, "Tom", true},
		{6, "Top", false},
		{7, "ZZZ", false},
	})
	_, err = testT.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	// Delete a row to make sure that soft deleted rows do not cause a problem
	where := sqtables.NewOpExpr(
		sqtables.NewColExpr(
			sqtables.NewColDef("rowid", tokens.Int, false)),
		tokens.Equal,
		sqtables.NewValueExpr(sqtypes.NewSQInt(7)),
	)
	err = where.ValidateCols(profile, tables)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	_, err = testT.DeleteRows(profile, where)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	data := []RowPtrsTest{
		{
			TestName: "All Rows no Cond",
			Tab:      testT,
			Where:    nil,
			ExpErr:   "",
			ExpPtrs:  sqptr.SQPtrs{1, 2, 3, 4, 5, 6},
			Sort:     true,
		},
		{
			TestName: "All Rows with Cond",
			Tab:      testT,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("rowid", tokens.Int, false)),
				tokens.LessThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(50)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{1, 2, 3, 4, 5, 6},
			Sort:    true,
		},
		{
			TestName: "No Rows",
			Tab:      testT,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("rowid", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(26)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{},
			Sort:    true,
		},
		{
			TestName: "First Row",
			Tab:      testT,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("rowid", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{1},
			Sort:    true,
		},
		{
			TestName: "Last Row",
			Tab:      testT,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("active", tokens.Bool, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQBool(false)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{6},
			Sort:    true,
		},
		{
			TestName: "Half the Rows",
			Tab:      testT,
			Where: sqtables.NewOpExpr(
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewColExpr(
							sqtables.NewColDef("rowid", tokens.Int, false)),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
					),
					tokens.Or,
					sqtables.NewOpExpr(
						sqtables.NewColExpr(
							sqtables.NewColDef("rowid", tokens.Int, false)),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQInt(3)),
					),
				),
				tokens.Or,
				sqtables.NewOpExpr(
					sqtables.NewColExpr(
						sqtables.NewColDef("active", tokens.Bool, false)),
					tokens.Equal,
					sqtables.NewValueExpr(sqtypes.NewSQBool(false)),
				),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{1, 3, 6},
			Sort:    true,
		},
		{
			TestName: "Condition type mismatch",
			Tab:      testT,
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("rowid", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQString("TEST")),
			),
			ExpErr:  "Error: Type Mismatch: TEST is not an Int",
			ExpPtrs: sqptr.SQPtrs{},
			Sort:    true,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetRowPtrsFunc(profile, &row))

	}
}

func TestMisc(t *testing.T) {

	profile := sqprofile.CreateSQProfile()
	// Data Setup
	tableName := "rowcounttest"
	tab := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("rowid", tokens.Int, true),
		sqtables.NewColDef("firstname", tokens.String, false),
		sqtables.NewColDef("active", tokens.Bool, false),
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	t.Run("RowCount:No Rows", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		num, err := tab.RowCount(profile)
		if err != nil {
			t.Error(err)
			return
		}
		if num != 0 {
			t.Errorf("RowCount = %d when it should be zero", num)
			return
		}
	})

	tables := sqtables.NewTableListFromTableDef(profile, tab)
	cols := sqtables.ColsToExpr(tab.GetCols(profile))
	dsData, err := sqtables.NewDataSet(profile, tables, cols)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, "Tim", true},
		{2, "Ted", true},
		{3, "Tex", true},
		{4, "Tad", true},
		{5, "Tom", true},
		{6, "Top", false},
		{7, "ZZZ", false},
	})
	_, err = tab.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	// Delete a row to make sure that soft deleted rows do not cause a problem
	where := sqtables.NewOpExpr(
		sqtables.NewColExpr(
			sqtables.NewColDef("rowid", tokens.Int, false)),
		tokens.Equal,
		sqtables.NewValueExpr(sqtypes.NewSQInt(7)),
	)
	err = where.ValidateCols(profile, tables)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	_, err = tab.DeleteRows(profile, where)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	t.Run("RowCount:6 Rows", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		num, err := tab.RowCount(profile)
		if err != nil {
			t.Error(err)
			return
		}
		if num != 6 {
			t.Errorf("RowCount = %d when it should be 6", num)
			return
		}
	})

	t.Run("String", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		expstr := "rowcounttest\n--------------------------------------\n\t{rowid, INT NOT NULL}\n\t{firstname, STRING}\n\t{active, BOOL}\n"
		str := tab.String(profile)
		if str != expstr {
			t.Errorf("String = %q \n\n\twhen it should be %q", str, expstr)
			return
		}
	})

	t.Run("GetRow invalid", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		row := tab.GetRow(profile, 0)
		if row != nil {
			t.Errorf("GetRow returned a row when it should be nil")
			return
		}
	})
}

type DeleteRowsData struct {
	TestName string
	Where    sqtables.Expr
	ExpErr   string
	ExpPtrs  sqptr.SQPtrs
}

func testDeleteRowsFunc(tableName string, d *DeleteRowsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		//Reset Data
		profile := sqprofile.CreateSQProfile()

		sqtables.DropTable(profile, tableName)

		// Data Setup
		tab := sqtables.CreateTableDef(tableName,
			sqtables.NewColDef("rownum", tokens.Int, false),
			sqtables.NewColDef("col1", tokens.Int, false),
			sqtables.NewColDef("col2", tokens.String, false),
			sqtables.NewColDef("col3", tokens.Int, false),
			sqtables.NewColDef("col4", tokens.Bool, false),
		)
		err := sqtables.CreateTable(profile, tab)
		if err != nil {
			t.Error("Error creating table: ", err)
			return
		}

		tables := sqtables.NewTableListFromTableDef(profile, tab)
		cols := sqtables.ColsToExpr(tab.GetCols(profile))
		dsData, err := sqtables.NewDataSet(profile, tables, cols)
		if err != nil {
			t.Error("Error setting up table: ", err)
			return
		}
		dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
			{1, 5, "d test string", 10, true},
			{2, 7, "f test string", 100, false},
			{3, 17, "A test string", 500, false},
		})
		_, err = tab.AddRows(profile, dsData)
		if err != nil {
			t.Error("Error setting up table: ", err)
			return
		}

		// Delete a row to make sure that soft deleted rows do not cause a problem
		where := sqtables.NewOpExpr(
			sqtables.NewColExpr(
				sqtables.NewColDef("rownum", tokens.Int, false)),
			tokens.Equal,
			sqtables.NewValueExpr(sqtypes.NewSQInt(3)),
		)
		err = where.ValidateCols(profile, tables)
		if err != nil {
			t.Error("Error setting up table: ", err)
			return
		}
		_, err = tab.DeleteRows(profile, where)
		if err != nil {
			t.Error("Error setting up table: ", err)
			return
		}

		if d.Where != nil {
			err = d.Where.ValidateCols(profile, tables)
			if err != nil {
				t.Errorf("Unable to validate cols in Where  %q", d.Where)
				return
			}
		}

		actPtrs, err := tab.DeleteRows(profile, d.Where)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if len(d.ExpPtrs) != len(actPtrs) {
			t.Errorf("The number of ptrs returned (%d) does not match expected ptrs (%d)", len(actPtrs), len(d.ExpPtrs))
			return
		}

		if len(actPtrs) == 0 && len(d.ExpPtrs) == 0 {
			return
		}
		sort.Slice(actPtrs, func(i, j int) bool { return actPtrs[i] < actPtrs[j] })
		sort.Slice(d.ExpPtrs, func(i, j int) bool { return d.ExpPtrs[i] < d.ExpPtrs[j] })
		if !reflect.DeepEqual(actPtrs, d.ExpPtrs) {
			t.Errorf("Actual Pointers %v do not match Expected Ptrs %v", actPtrs, d.ExpPtrs)
			return
		}
	}
}

func TestDeleteRows(t *testing.T) {
	tableName := "rowdeletetest"
	//	col1Def := sqtables.NewColExpr(*testT.FindColDef(profile, "col1"))
	testData := []DeleteRowsData{
		{
			TestName: "col1(5) = 5 ->1",
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{1},
		},
		{
			TestName: "col1(6) = 5 ->0",
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(6)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{},
		},
		{
			TestName: "col1 < 5 ->0",
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.LessThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{},
		},
		{
			TestName: "col1 < 7 ->1",
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.LessThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(6)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{1},
		},
		{
			TestName: "Delete where=nil",
			Where:    nil,
			ExpErr:   "",
			ExpPtrs:  sqptr.SQPtrs{1, 2},
		},
		{
			TestName: "Delete where error",
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					sqtables.NewColDef("col2", tokens.String, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr: "Error: Type Mismatch: 5 is not a String", ExpPtrs: sqptr.SQPtrs{1, 2},
		},
	}

	for i, row := range testData {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName), testDeleteRowsFunc(tableName, &row))
	}

	//Do a hard Delete
	t.Run("DeleteFromPtrs HardDelete", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()

		tab, err := sqtables.GetTable(profile, tableName)
		if err != nil {
			t.Error(err)
			return
		}

		err = tab.DeleteRowsFromPtrs(profile, sqptr.SQPtrs{1, 2, 3}, sqtables.HardDelete)
		if err != nil {
			t.Errorf("Unable to hard delete from table")
			return
		}
	})

}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type GetRowDataFromPtrsData struct {
	TestName string
	Tab      *sqtables.TableDef
	ExpErr   string
	Ptrs     sqptr.SQPtrs
	ExpVals  sqtypes.RawVals
}

func testGetRowDataFromPtrsFunc(d *GetRowDataFromPtrsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()

		data, err := d.Tab.GetRowDataFromPtrs(profile, d.Ptrs)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if len(d.Ptrs) != data.Len() {
			t.Errorf("The number of ptrs (%d) does not match data returned (%d)", len(d.Ptrs), data.Len())
			return
		}

		expVals := sqtypes.CreateValuesFromRaw(d.ExpVals)
		msg := sqtypes.Compare2DValue(expVals, data.Vals, "Expected Values", "Actual Values", false)
		if msg != "" {
			t.Error(msg)
			return
		}
	}
}

func TestGetRowDataFromPtrs(t *testing.T) {

	profile := sqprofile.CreateSQProfile()
	tableName := "RowDataFromPtrstest"

	// Data Setup
	tab := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("rownum", tokens.Int, false),
		sqtables.NewColDef("col1", tokens.Int, false),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.Bool, false),
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	tables := sqtables.NewTableListFromTableDef(profile, tab)
	cols := sqtables.ColsToExpr(tab.GetCols(profile))
	dsData, err := sqtables.NewDataSet(profile, tables, cols)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, 5, "d test string", 10, true},
		{2, 7, "f test string", 100, false},
		{3, 5, "a test string", 10, true},
		{4, 7, "b test string", 100, false},
		{5, 5, "c test string", 10, true},
		{6, 7, "e test string", 100, false},
	})
	_, err = tab.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	//	col1Def := sqtables.NewColExpr(*testT.FindColDef(profile, "col1"))
	testData := []GetRowDataFromPtrsData{
		{
			TestName: "All Rows",
			Tab:      tab,
			ExpErr:   "",
			Ptrs:     sqptr.SQPtrs{1, 2, 3, 4, 5, 6},
			ExpVals: sqtypes.RawVals{
				{1, 5, "d test string", 10, true},
				{2, 7, "f test string", 100, false},
				{3, 5, "a test string", 10, true},
				{4, 7, "b test string", 100, false},
				{5, 5, "c test string", 10, true},
				{6, 7, "e test string", 100, false},
			},
		},
		{
			TestName: "Invalid Ptr",
			Tab:      tab,
			ExpErr:   "Error: Row 11 does not exist",
			Ptrs:     sqptr.SQPtrs{11, 2, 3, 4, 5, 6},
		},
		{
			TestName: "Some Rows",
			Tab:      tab,
			ExpErr:   "",
			Ptrs:     sqptr.SQPtrs{1, 3, 6},
			ExpVals: sqtypes.RawVals{
				{1, 5, "d test string", 10, true},
				{3, 5, "a test string", 10, true},
				{6, 7, "e test string", 100, false},
			},
		},
	}

	for i, row := range testData {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName), testGetRowDataFromPtrsFunc(&row))
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type UpdateRowsFromPtrsData struct {
	TestName string
	Tab      *sqtables.TableDef
	ExpErr   string
	Ptrs     sqptr.SQPtrs
	Cols     []string
	ExpList  *sqtables.ExprList
	ExpData  sqtypes.RawVals
}

func testUpdateRowsFromPtrsFunc(d *UpdateRowsFromPtrsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()

		err := d.Tab.UpdateRowsFromPtrs(profile, d.Ptrs, d.Cols, d.ExpList)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if d.ExpData != nil {
			cList := sqtables.ColsToExpr(d.Tab.GetCols(profile))
			ds, err := d.Tab.GetRowData(profile, cList, nil, nil, nil)
			if err != nil {
				t.Errorf("Error getting data for comparison: %s", err)
				return
			}
			v := ds.Vals
			sort.SliceStable(v, func(i, j int) bool { return v[i][1].LessThan(v[j][1]) })
			sort.SliceStable(v, func(i, j int) bool { return v[i][0].LessThan(v[j][0]) })
			expVals := sqtypes.CreateValuesFromRaw(d.ExpData)
			if !reflect.DeepEqual(v, expVals) {
				t.Error("Expected data does not match actual data in table")
				fmt.Printf("Actual: \n%v, \n\nExpected:\n%v", v, expVals)
				return
			}
		}
	}
}

func TestUpdateRowsFromPtrs(t *testing.T) {

	profile := sqprofile.CreateSQProfile()
	tableName := "UpdateRowsFromPtrstest"

	// Data Setup
	tab := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("rownum", tokens.Int, false),
		sqtables.NewColDef("col1", tokens.Int, false),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.Bool, false),
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	tables := sqtables.NewTableListFromTableDef(profile, tab)
	cols := sqtables.ColsToExpr(tab.GetCols(profile))
	dsData, err := sqtables.NewDataSet(profile, tables, cols)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, 5, "d test string", 10, true},
		{2, 7, "f test string", 100, false},
		{3, 5, "a test string", 10, true},
		{4, 7, "b test string", 100, false},
		{5, 5, "c test string", 10, true},
		{6, 7, "e test string", 100, false},
	})
	_, err = tab.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	//	col1Def := sqtables.NewColExpr(*testT.FindColDef(profile, "col1"))
	testData := []UpdateRowsFromPtrsData{
		{
			TestName: "All Rows col4",
			Tab:      tab,
			ExpErr:   "",
			Ptrs:     sqptr.SQPtrs{1, 2, 3, 4, 5, 6},
			Cols:     []string{"col4"},
			ExpList:  sqtables.NewExprList(sqtables.NewValueExpr(sqtypes.NewSQBool(true))),
			ExpData: sqtypes.RawVals{
				{1, 5, "d test string", 10, true},
				{2, 7, "f test string", 100, true},
				{3, 5, "a test string", 10, true},
				{4, 7, "b test string", 100, true},
				{5, 5, "c test string", 10, true},
				{6, 7, "e test string", 100, true},
			},
		},
		{
			TestName: "Invalid Ptr",
			Tab:      tab,
			ExpErr:   "Internal Error: Row 11 does not exist for update",
			Ptrs:     sqptr.SQPtrs{11, 2, 3, 4, 5, 6},
			Cols:     []string{"col4"},
			ExpList:  sqtables.NewExprList(sqtables.NewValueExpr(sqtypes.NewSQBool(true))),
		},
		{
			TestName: "UpdateRow Error:Type Mismatch",
			Tab:      tab,
			ExpErr:   "Error: Type Mismatch: Column col4 in Table updaterowsfromptrstest has a type of BOOL, Unable to set value of type INT",
			Ptrs:     sqptr.SQPtrs{1},
			Cols:     []string{"col4"},
			ExpList:  sqtables.NewExprList(sqtables.NewValueExpr(sqtypes.NewSQInt(55))),
			ExpData: sqtypes.RawVals{
				{1, 5, "d test string", 10, true},
				{2, 7, "f test string", 100, true},
				{3, 5, "a test string", 10, true},
				{4, 7, "b test string", 100, true},
				{5, 5, "c test string", 10, true},
				{6, 7, "e test string", 100, true},
			},
		},
		{
			TestName: "Evaluate Error",
			Tab:      tab,
			ExpErr:   "Error: Column \"ColX\" not found in Table(s): updaterowsfromptrstest",
			Ptrs:     sqptr.SQPtrs{1},
			Cols:     []string{"col4"},
			ExpList:  sqtables.NewExprList(sqtables.NewColExpr(sqtables.NewColDef("ColX", tokens.Float, false))),
			ExpData: sqtypes.RawVals{
				{1, 5, "d test string", 10, true},
				{2, 7, "f test string", 100, true},
				{3, 5, "a test string", 10, true},
				{4, 7, "b test string", 100, true},
				{5, 5, "c test string", 10, true},
				{6, 7, "e test string", 100, true},
			},
		},
	}

	for i, row := range testData {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName), testUpdateRowsFromPtrsFunc(&row))
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type AddRowsData struct {
	TestName string
	Tab      *sqtables.TableDef
	ExpErr   string
	Cols     []string
	ExpData  sqtypes.RawVals
}

func testAddRowsFunc(d *AddRowsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		clist := sqtables.NewColListNames(d.Cols)
		tables := sqtables.NewTableListFromTableDef(profile, d.Tab)
		err := clist.Validate(profile, tables)
		if err != nil {
			t.Errorf("Unexpected Error setting up ColList for test %s: %s", t.Name(), err)
		}
		data, err := sqtables.NewDataSet(profile, tables, sqtables.ColsToExpr(d.Tab.GetCols(profile)))
		if err != nil {
			t.Errorf("Unexpected Error setting up DataSet for test %s: %s", t.Name(), err)
		}
		data.Vals = sqtypes.CreateValuesFromRaw(d.ExpData)
		n, err := d.Tab.AddRows(profile, data)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if n != len(d.ExpData) {
			t.Errorf("Number of rows returned %d does not match expected %d", n, len(d.ExpData))
		}

		if d.ExpData != nil {
			cList := sqtables.ColsToExpr(d.Tab.GetCols(profile))
			ds, err := d.Tab.GetRowData(profile, cList, nil, nil, nil)
			if err != nil {
				t.Errorf("Error getting data for comparison: %s", err)
				return
			}
			v := ds.Vals
			sort.SliceStable(v, func(i, j int) bool { return v[i][1].LessThan(v[j][1]) })
			sort.SliceStable(v, func(i, j int) bool { return v[i][0].LessThan(v[j][0]) })
			expVals := sqtypes.CreateValuesFromRaw(d.ExpData)
			if !reflect.DeepEqual(v, expVals) {
				t.Error("Expected data does not match actual data in table")
				fmt.Printf("Actual: \n%v, \n\nExpected:\n%v", v, expVals)
				return
			}
		}

	}
}

func TestAddRows(t *testing.T) {

	profile := sqprofile.CreateSQProfile()
	tableName := "AddRowstest"

	// Data Setup
	tab := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("rownum", tokens.Int, false),
		sqtables.NewColDef("col1", tokens.Int, false),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.Bool, false),
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	//	col1Def := sqtables.NewColExpr(*testT.FindColDef(profile, "col1"))
	testData := []AddRowsData{
		{
			TestName: "Add 6 Rows",
			Tab:      tab,
			ExpErr:   "",
			Cols:     []string{"rownum", "col1", "col2", "col3", "col4"},
			ExpData: sqtypes.RawVals{
				{1, 5, "d test string", 10, true},
				{2, 7, "f test string", 100, false},
				{3, 5, "a test string", 10, true},
				{4, 7, "b test string", 100, false},
				{5, 5, "c test string", 10, true},
				{6, 7, "e test string", 100, false},
			},
		},
		{
			TestName: "Add Long Row",
			Tab:      tab,
			ExpErr:   "Error: The Number of Columns (5) does not match the number of Values (6)",
			Cols:     []string{"rownum", "col1", "col2", "col3", "col4"},
			ExpData: sqtypes.RawVals{
				{7, 5, "d test string", 10, true, 55},
			},
		}}

	for i, row := range testData {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName), testAddRowsFunc(&row))
	}
}
