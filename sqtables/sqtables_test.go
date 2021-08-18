package sqtables_test

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/wilphi/sqsrv/assertions"
	"github.com/wilphi/sqsrv/sqmutex"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

const (
	withErr    = true
	withoutErr = false
)

func TestMain(m *testing.M) {
	sqmutex.DefaultTimeout = time.Millisecond * 100
	os.Exit(m.Run())
}
func init() {
	sqtest.TestInit("sqtables_test.log")
}

type RowDataTest struct {
	TestName       string
	Tab            *sqtables.TableRef
	EList          *sqtables.ExprList
	WhereExpr      sqtables.Expr
	ExpErr         string
	ExpPtrs        []int
	SkipRowNumTest bool
	LockTest       bool
}

func testGetRowDataFunc(profile *sqprofile.SQProfile, d *RowDataTest) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if d.LockTest {
			p2 := sqprofile.CreateSQProfile()
			d.Tab.Table.Lock(p2)
			defer d.Tab.Table.Unlock(p2)
		}
		if d.WhereExpr != nil {
			err := d.WhereExpr.ValidateCols(profile, sqtables.NewTableListFromTableDef(profile, d.Tab.Table))
			assertions.AssertNoErr(err, "Unable to validate where clause")
		}

		data, err := d.Tab.GetRowData(profile, d.EList, d.WhereExpr)
		if sqtest.CheckErrContain(t, err, d.ExpErr) {
			return
		}

		if len(d.ExpPtrs) != data.Len() {
			t.Errorf("The number of rows returned (%d) does not match expected rows (%d)", data.Len(), len(d.ExpPtrs))
			return
		}

		// make sure the row numbers match
		if !d.SkipRowNumTest {
			for i := range data.Vals {
				if !data.Vals[i][0].Equal(sqtypes.NewSQInt(d.ExpPtrs[i])) {
					t.Errorf("Returned Row num (%d) does not match expected (%d)", data.Vals[i][0], d.ExpPtrs[i])
				}
			}
		}
	}
}

func TestGetRowData(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	profile := sqprofile.CreateSQProfile()
	// Data Setup
	tableName := "rowdatatest"
	testT := sqtables.CreateTableDef(tableName,
		[]column.Def{
			column.NewDef("rownum", tokens.Int, false),
			column.NewDef("col1", tokens.Int, false),
			column.NewDef("col2", tokens.String, false),
			column.NewDef("col3", tokens.Int, false),
			column.NewDef("col4", tokens.Bool, false),
		},
	)
	err := sqtables.CreateTable(profile, testT)
	assertions.AssertNoErr(err, "Error creating table")

	tr, err := sqtables.GetTableRef(profile, tableName)
	assertions.AssertNoErr(err, "Unable to get tableRef")

	tables := sqtables.NewTableListFromTableDef(profile, testT)
	cols := sqtables.ColsToExpr(testT.GetCols(profile))
	dsData, err := sqtables.NewDataSet(profile, tables, cols)
	assertions.AssertNoErr(err, "Error setting up table")

	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, 5, "d test string", 10, true},
		{2, 7, "f test string", 100, false},
		{3, 17, "zz test string", 700, true},
	})
	_, err = testT.AddRows(sqtables.BeginTrans(profile, true), dsData)
	assertions.AssertNoErr(err, "Error setting up table")

	// Delete a row to make sure that soft deleted rows do not cause a problem
	where := sqtables.NewOpExpr(
		sqtables.NewColExpr(
			column.NewRef("rownum", tokens.Int, false)),
		tokens.Equal,
		sqtables.NewValueExpr(sqtypes.NewSQInt(3)),
	)
	err = where.ValidateCols(profile, tables)
	assertions.AssertNoErr(err, "Error setting up table")

	_, err = testT.DeleteRows(sqtables.BeginTrans(profile, true), where)
	assertions.AssertNoErr(err, "Error setting up table")

	testData := []RowDataTest{
		{
			TestName: "col1(5) = 5 ->1",
			Tab:      tr,
			EList:    cols,
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),

			ExpErr:  "",
			ExpPtrs: []int{1},
		},
		{
			TestName: "Lock Test",
			Tab:      tr,
			EList:    cols,
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr:   "Table: rowdatatest Read Lock failed due to timeout:",
			ExpPtrs:  []int{1},
			LockTest: true,
		},
		{
			TestName: "col1(6) = 5 ->0",
			Tab:      tr,
			EList:    cols,
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(6)),
			),
			ExpErr:  "",
			ExpPtrs: []int{},
		},
		{
			TestName: "col1 < 5 ->0",
			Tab:      tr,
			EList:    cols,
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col1", tokens.Int, false)),
				tokens.LessThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr:  "",
			ExpPtrs: []int{},
		},
		{
			TestName: "col1 < 7 ->1",
			Tab:      tr,
			EList:    cols,
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col1", tokens.Int, false)),
				tokens.LessThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(6)),
			),

			ExpErr:  "",
			ExpPtrs: []int{1},
		},
		{
			TestName: "Where Error",
			Tab:      tr,
			EList:    cols,
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col2", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(6)),
			),
			ExpErr:  "Error: Type Mismatch: 6 is not a String",
			ExpPtrs: []int{1},
		},

		{
			TestName: "Invalid Col in Expression",
			Tab:      tr,
			EList: sqtables.NewExprList(
				sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
				sqtables.NewColExpr(column.NewRef("colX", tokens.String, false)),
			),
			WhereExpr: nil,

			ExpErr:  "Error: Column \"colX\" not found in Table(s): rowdatatest",
			ExpPtrs: []int{2},
		},
		{
			TestName: "Invalid function in Expression on Evaluate",
			Tab:      tr,
			EList: sqtables.NewExprList(
				sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
				sqtables.NewFuncExpr(
					tokens.Float,
					sqtables.NewColExpr(column.NewRef("col2", tokens.String, false)),
				),
			),
			WhereExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col2", tokens.Int, false)),
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
		[]column.Def{
			column.NewDef("rowid", tokens.Int, false),
			column.NewDef("firstname", tokens.String, false),
			column.NewDef("active", tokens.Bool, false),
		},
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
	trans := sqtables.BeginTrans(profile, true)

	_, err = testT.AddRows(trans, dsData)
	if err != nil {
		trans.Rollback()
		t.Error("Error setting up table: ", err)
		return
	}
	err = trans.Commit()
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	// Delete a row to make sure that soft deleted rows do not cause a problem
	where := sqtables.NewOpExpr(
		sqtables.NewColExpr(
			column.NewRef("rowid", tokens.Int, false)),
		tokens.Equal,
		sqtables.NewValueExpr(sqtypes.NewSQInt(7)),
	)
	err = where.ValidateCols(profile, tables)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	_, err = testT.DeleteRows(sqtables.BeginTrans(profile, true), where)
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
					column.NewRef("rowid", tokens.Int, false)),
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
					column.NewRef("rowid", tokens.Int, false)),
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
					column.NewRef("rowid", tokens.Int, false)),
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
					column.NewRef("active", tokens.Bool, false)),
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
							column.NewRef("rowid", tokens.Int, false)),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
					),
					tokens.Or,
					sqtables.NewOpExpr(
						sqtables.NewColExpr(
							column.NewRef("rowid", tokens.Int, false)),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQInt(3)),
					),
				),
				tokens.Or,
				sqtables.NewOpExpr(
					sqtables.NewColExpr(
						column.NewRef("active", tokens.Bool, false)),
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
					column.NewRef("rowid", tokens.Int, false)),
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
		[]column.Def{
			column.NewDef("rowid", tokens.Int, true),
			column.NewDef("firstname", tokens.String, false),
			column.NewDef("active", tokens.Bool, false),
		},
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
	trans := sqtables.BeginTrans(profile, true)
	_, err = tab.AddRows(trans, dsData)
	if err != nil {
		trans.Rollback()
		t.Error("Error setting up table: ", err)
		return
	}
	err = trans.Commit()
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	// Delete a row to make sure that soft deleted rows do not cause a problem
	where := sqtables.NewOpExpr(
		sqtables.NewColExpr(
			column.NewRef("rowid", tokens.Int, false)),
		tokens.Equal,
		sqtables.NewValueExpr(sqtypes.NewSQInt(7)),
	)
	err = where.ValidateCols(profile, tables)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	_, err = tab.DeleteRows(sqtables.BeginTrans(profile, true), where)
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

////////////////////////////////////////////////////////////////////////////////////////////////////

type DeleteRowsData struct {
	TestName string
	Where    sqtables.Expr
	ExpErr   string
	ExpPtrs  sqptr.SQPtrs
	LockTest bool
}

func testDeleteRowsFunc(tableName string, d *DeleteRowsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		//Reset Data
		profile := sqprofile.CreateSQProfile()

		sqtables.DropTable(profile, tableName)

		// Data Setup
		tab := sqtables.CreateTableDef(tableName,
			[]column.Def{
				column.NewDef("rownum", tokens.Int, false),
				column.NewDef("col1", tokens.Int, false),
				column.NewDef("col2", tokens.String, false),
				column.NewDef("col3", tokens.Int, false),
				column.NewDef("col4", tokens.Bool, false),
			},
		)
		err := sqtables.CreateTable(profile, tab)
		assertions.AssertNoErr(err, "Error creating table")

		tables := sqtables.NewTableListFromTableDef(profile, tab)
		cols := sqtables.ColsToExpr(tab.GetCols(profile))
		dsData, err := sqtables.NewDataSet(profile, tables, cols)
		assertions.AssertNoErr(err, "Error setting up table")

		dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
			{1, 5, "d test string", 10, true},
			{2, 7, "f test string", 100, false},
			{3, 17, "A test string", 500, false},
		})
		_, err = tab.AddRows(sqtables.BeginTrans(profile, true), dsData)
		assertions.AssertNoErr(err, "Error setting up table")

		// Delete a row to make sure that soft deleted rows do not cause a problem
		where := sqtables.NewOpExpr(
			sqtables.NewColExpr(
				column.NewRef("rownum", tokens.Int, false)),
			tokens.Equal,
			sqtables.NewValueExpr(sqtypes.NewSQInt(3)),
		)
		err = where.ValidateCols(profile, tables)
		assertions.AssertNoErr(err, "Error setting up table")

		_, err = tab.DeleteRows(sqtables.BeginTrans(profile, true), where)
		assertions.AssertNoErr(err, "Error setting up table")

		if d.Where != nil {
			err = d.Where.ValidateCols(profile, tables)
			if err != nil {
				t.Errorf("Unable to validate cols in Where  %q", d.Where)
				return
			}
		}
		if d.LockTest {
			p2 := sqprofile.CreateSQProfile()
			tab.Lock(p2)
			defer tab.Unlock(p2)
		}

		actPtrs, err := tab.DeleteRows(sqtables.BeginTrans(profile, true), d.Where)
		if sqtest.CheckErrContain(t, err, d.ExpErr) {
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
	defer sqtest.PanicTestRecovery(t, "")

	tableName := "rowdeletetest"
	//	col1Def := sqtables.NewColExpr(*testT.FindColDef(profile, "col1"))
	testData := []DeleteRowsData{
		{
			TestName: "col1(5) = 5 ->1",
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr:  "",
			ExpPtrs: sqptr.SQPtrs{1},
		},
		{
			TestName: "LockTest",
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col1", tokens.Int, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr:   "Table: rowdeletetest Write Lock failed due to timeout:",
			ExpPtrs:  sqptr.SQPtrs{1},
			LockTest: true,
		},
		{
			TestName: "col1(6) = 5 ->0",
			Where: sqtables.NewOpExpr(
				sqtables.NewColExpr(
					column.NewRef("col1", tokens.Int, false)),
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
					column.NewRef("col1", tokens.Int, false)),
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
					column.NewRef("col1", tokens.Int, false)),
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
					column.NewRef("col2", tokens.String, false)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpErr: "Error: Type Mismatch: 5 is not a String", ExpPtrs: sqptr.SQPtrs{1, 2},
		},
	}

	for i, row := range testData {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName), testDeleteRowsFunc(tableName, &row))
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type DeleteRowsFromPtrsData struct {
	TestName     string
	Ptrs         sqptr.SQPtrs
	ExpErr       string
	ExpVals      sqtypes.RawVals
	LockTest     bool
	InvalidTrans bool
}

func testDeleteRowsFromPtrsFunc(tableName string, d *DeleteRowsFromPtrsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		//Reset Data
		profile := sqprofile.CreateSQProfile()

		sqtables.DropTable(profile, tableName)

		// Data Setup
		tab := sqtables.CreateTableDef(tableName,
			[]column.Def{
				column.NewDef("rownum", tokens.Int, false),
				column.NewDef("col1", tokens.Int, false),
				column.NewDef("col2", tokens.String, false),
			},
		)
		err := sqtables.CreateTable(profile, tab)
		assertions.AssertNoErr(err, "Error creating table")

		tables := sqtables.NewTableListFromTableDef(profile, tab)
		cols := sqtables.ColsToExpr(tab.GetCols(profile))
		dsData, err := sqtables.NewDataSet(profile, tables, cols)
		assertions.AssertNoErr(err, "Error setting up table")

		dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
			{1, 25, "row1"},
			{2, 50, "row2"},
			{3, 75, "row3"},
			{4, 100, "row4"},
			{5, 125, "row5"},
		})
		_, err = tab.AddRows(sqtables.BeginTrans(profile, true), dsData)
		assertions.AssertNoErr(err, "Error setting up table")

		if d.LockTest {
			p2 := sqprofile.CreateSQProfile()
			tab.Lock(p2)
			defer tab.Unlock(p2)
		}

		trans := sqtables.BeginTrans(profile, true)
		if d.InvalidTrans {
			trans.Rollback()
		}
		err = tab.DeleteRowsFromPtrs(trans, d.Ptrs)
		if sqtest.CheckErrContain(t, err, d.ExpErr) {
			return
		}
		tr := tab.TableRef(profile)
		data, err := tr.GetRowData(profile, sqtables.ColsToExpr(tab.GetCols(profile)), nil)
		assertions.AssertNoErr(err, "Error Verifying data")

		expVals := d.ExpVals.ValueMatrix()
		if str := sqtypes.CompareValueMatrix(data.Vals, expVals, "Actual", "Expected", true); str != "" {
			t.Error(str)
			return
		}
	}
}

func TestDeleteRowsFromPtrs(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	tableName := "rowdeletetestfromptrs"
	//	col1Def := sqtables.NewColExpr(*testT.FindColDef(profile, "col1"))
	testData := []DeleteRowsFromPtrsData{
		{
			TestName: "LockTest",
			ExpErr:   "Table: rowdeletetestfromptrs Write Lock failed due to timeout:",
			LockTest: true,
		},
		{
			TestName: "Empty Ptr list",
			ExpErr:   "",
			LockTest: false,
			Ptrs:     sqptr.SQPtrs{},
			ExpVals: sqtypes.RawVals{
				{1, 25, "row1"},
				{2, 50, "row2"},
				{3, 75, "row3"},
				{4, 100, "row4"},
				{5, 125, "row5"},
			},
		},
		{
			TestName: "Delete first Row",
			ExpErr:   "",
			LockTest: false,
			Ptrs:     sqptr.SQPtrs{1},
			ExpVals: sqtypes.RawVals{
				{2, 50, "row2"},
				{3, 75, "row3"},
				{4, 100, "row4"},
				{5, 125, "row5"},
			},
		},
		{
			TestName: "Delete Last Row",
			ExpErr:   "",
			LockTest: false,
			Ptrs:     sqptr.SQPtrs{5},
			ExpVals: sqtypes.RawVals{
				{1, 25, "row1"},
				{2, 50, "row2"},
				{3, 75, "row3"},
				{4, 100, "row4"},
			},
		},
		{
			TestName: "Invalid Ptr",
			ExpErr:   "Internal Error: Row Ptr 100 does not exist",
			LockTest: false,
			Ptrs:     sqptr.SQPtrs{100},
		},
		{
			TestName:     "Transaction Delete Error",
			ExpErr:       "Internal Error: Transaction is already complete",
			LockTest:     false,
			Ptrs:         sqptr.SQPtrs{100},
			InvalidTrans: true,
		},
	}

	for i, row := range testData {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName), testDeleteRowsFromPtrsFunc(tableName, &row))
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type HardDeleteRowsFromPtrsData struct {
	TestName string
	Ptrs     sqptr.SQPtrs
	ExpErr   string
	ExpVals  sqtypes.RawVals
	LockTest bool
}

func testHardDeleteRowsFromPtrsFunc(tableName string, d *HardDeleteRowsFromPtrsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		//Reset Data
		profile := sqprofile.CreateSQProfile()

		sqtables.DropTable(profile, tableName)

		// Data Setup
		tab := sqtables.CreateTableDef(tableName,
			[]column.Def{
				column.NewDef("rownum", tokens.Int, false),
				column.NewDef("col1", tokens.Int, false),
				column.NewDef("col2", tokens.String, false),
			},
		)
		err := sqtables.CreateTable(profile, tab)
		assertions.AssertNoErr(err, "Error creating table")

		tables := sqtables.NewTableListFromTableDef(profile, tab)
		cols := sqtables.ColsToExpr(tab.GetCols(profile))
		dsData, err := sqtables.NewDataSet(profile, tables, cols)
		assertions.AssertNoErr(err, "Error setting up table")

		dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
			{1, 25, "row1"},
			{2, 50, "row2"},
			{3, 75, "row3"},
			{4, 100, "row4"},
			{5, 125, "row5"},
		})
		_, err = tab.AddRows(sqtables.BeginTrans(profile, true), dsData)
		assertions.AssertNoErr(err, "Error setting up table")

		if d.LockTest {
			p2 := sqprofile.CreateSQProfile()
			tab.Lock(p2)
			defer tab.Unlock(p2)
		}

		err = tab.HardDeleteRowsFromPtrs(profile, d.Ptrs)
		if sqtest.CheckErrContain(t, err, d.ExpErr) {
			return
		}
		tr := tab.TableRef(profile)
		data, err := tr.GetRowData(profile, sqtables.ColsToExpr(tab.GetCols(profile)), nil)
		expVals := d.ExpVals.ValueMatrix()
		if str := sqtypes.CompareValueMatrix(data.Vals, expVals, "Actual", "Expected", true); str != "" {
			t.Error(str)
			return
		}
		rowCnt, err := tab.RowCount(profile)
		assertions.AssertNoErr(err, "Error counting rows")
		rawCnt, err := tab.RawCount(profile)
		assertions.AssertNoErr(err, "Error raw count of rows")
		if rowCnt != rawCnt {
			t.Errorf("RowCount (%d) != RawCount (%d)", rowCnt, rawCnt)
			return
		}
	}
}

func TestHardDeleteRowsFromPtrs(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	tableName := "rowdeletetestfromptrs"
	//	col1Def := sqtables.NewColExpr(*testT.FindColDef(profile, "col1"))
	testData := []HardDeleteRowsFromPtrsData{
		{
			TestName: "LockTest",
			ExpErr:   "Table: rowdeletetestfromptrs Write Lock failed due to timeout:",
			LockTest: true,
		},
		{
			TestName: "Empty Ptr list",
			ExpErr:   "",
			Ptrs:     sqptr.SQPtrs{},
			ExpVals: sqtypes.RawVals{
				{1, 25, "row1"},
				{2, 50, "row2"},
				{3, 75, "row3"},
				{4, 100, "row4"},
				{5, 125, "row5"},
			},
		},
		{
			TestName: "Delete first Row",
			ExpErr:   "",
			Ptrs:     sqptr.SQPtrs{1},
			ExpVals: sqtypes.RawVals{
				{2, 50, "row2"},
				{3, 75, "row3"},
				{4, 100, "row4"},
				{5, 125, "row5"},
			},
		},
		{
			TestName: "Delete Last Row",
			ExpErr:   "",
			Ptrs:     sqptr.SQPtrs{5},
			ExpVals: sqtypes.RawVals{
				{1, 25, "row1"},
				{2, 50, "row2"},
				{3, 75, "row3"},
				{4, 100, "row4"},
			},
		},
		{
			TestName: "Invalid Ptr",
			ExpErr:   "Internal Error: Row Ptr 100 does not exist",
			Ptrs:     sqptr.SQPtrs{100},
		},
	}

	for i, row := range testData {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName), testHardDeleteRowsFromPtrsFunc(tableName, &row))
	}
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
		[]column.Def{
			column.NewDef("rownum", tokens.Int, false),
			column.NewDef("col1", tokens.Int, false),
			column.NewDef("col2", tokens.String, false),
			column.NewDef("col3", tokens.Int, false),
			column.NewDef("col4", tokens.Bool, false),
		},
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
	trans := sqtables.BeginTrans(profile, true)
	_, err = tab.AddRows(trans, dsData)
	if err != nil {
		trans.Rollback()
		t.Error("Error setting up table: ", err)
		return
	}
	err = trans.Commit()
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

		err := d.Tab.UpdateRowsFromPtrs(sqtables.BeginTrans(profile, true), d.Ptrs, d.Cols, d.ExpList)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if d.ExpData != nil {
			q := sqtables.Query{
				Tables: sqtables.NewTableListFromTableDef(profile, d.Tab),
				EList:  sqtables.ColsToExpr(d.Tab.GetCols(profile)),
			}
			ds, err := q.GetRowData(profile)
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
				//fmt.Printf("Actual: \n%v, \n\nExpected:\n%v", v, expVals)
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
		[]column.Def{
			column.NewDef("rownum", tokens.Int, false),
			column.NewDef("col1", tokens.Int, false),
			column.NewDef("col2", tokens.String, false),
			column.NewDef("col3", tokens.Int, false),
			column.NewDef("col4", tokens.Bool, false),
		},
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
	trans := sqtables.BeginTrans(profile, true)
	_, err = tab.AddRows(trans, dsData)
	if err != nil {
		trans.Rollback()
		t.Error("Error setting up table: ", err)
		return
	}
	err = trans.Commit()
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
			ExpList:  sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("ColX", tokens.Float, false))),
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
		clist := column.NewListNames(d.Cols)
		tables := sqtables.NewTableListFromTableDef(profile, d.Tab)
		err := clist.Validate(profile, tables)
		if err != nil {
			t.Errorf("Unexpected Error setting up column.List for test %s: %s", t.Name(), err)
		}
		data, err := sqtables.NewDataSet(profile, tables, sqtables.ColsToExpr(d.Tab.GetCols(profile)))
		if err != nil {
			t.Errorf("Unexpected Error setting up DataSet for test %s: %s", t.Name(), err)
		}
		data.Vals = sqtypes.CreateValuesFromRaw(d.ExpData)
		trans := sqtables.BeginTrans(profile, false)
		defer trans.Rollback()
		n, err := d.Tab.AddRows(trans, data)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if n != len(d.ExpData) {
			t.Errorf("Number of rows returned %d does not match expected %d", n, len(d.ExpData))
		}

		if d.ExpData != nil {
			tt := trans.(*sqtables.STransaction)
			tbl := tt.TData[d.Tab.GetName(profile)]
			ds, err := tbl.TableRef(profile).GetRowData(profile, sqtables.ColsToExpr(tbl.GetCols(profile)), nil)
			if err != nil {
				t.Errorf("Unexpected error retrieving transaction data - %s", err)
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
		[]column.Def{
			column.NewDef("rownum", tokens.Int, false),
			column.NewDef("col1", tokens.Int, false),
			column.NewDef("col2", tokens.String, false),
			column.NewDef("col3", tokens.Int, false),
			column.NewDef("col4", tokens.Bool, false),
		},
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
