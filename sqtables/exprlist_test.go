package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

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

type EvalListData struct {
	TestName    string
	List        []sqtables.Expr
	profile     *sqprofile.SQProfile
	Tables      sqtables.TableList
	rows        []sqtables.RowInterface
	ExpVals     []sqtypes.Raw
	ExpErr      string
	UnValidated bool
}

func testEvalListFunc(d EvalListData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		eList := sqtables.NewExprList(d.List...)
		if !d.UnValidated {
			err := eList.ValidateCols(d.profile, d.Tables)
			if err != nil {
				t.Errorf("Unable to validate cols: %s", err.Error())
			}
		}
		retVals, err := eList.Evaluate(d.profile, sqtables.EvalFull, d.rows...)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if retVals == nil {
			if d.ExpVals != nil {
				t.Errorf("Actual values \"nil\" does not match Expected values %q", d.ExpVals)
			}
			return
		}
		evals := sqtypes.CreateValueArrayFromRaw(d.ExpVals)

		if !reflect.DeepEqual(sqtypes.ValueArray(retVals), evals) {
			t.Errorf("Actual values %q does not match Expected values %q", retVals, evals)
			return
		}
	}
}
func TestEvalListExpr(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tableName := "elisttest"
	tab := sqtables.CreateTableDef(tableName,
		[]column.Def{
			column.NewDef("col1", tokens.Int, false),
			column.NewDef("col2", tokens.String, false),
		},
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}
	tables := sqtables.NewTableListFromTableDef(profile, tab)
	dsData, err := sqtables.NewDataSet(profile, tables, sqtables.ColsToExpr(tab.GetCols(profile)))
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{{1, "test1"}, {2, "test2"}})

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

	row := tab.GetRow(profile, 1)
	rows := []sqtables.RowInterface{row}
	data := []EvalListData{
		{
			TestName: "Value Expr Int",
			List:     []sqtables.Expr{sqtables.NewValueExpr(sqtypes.NewSQInt(1234))},
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVals:  []sqtypes.Raw{1234},
			ExpErr:   "",
		},
		{
			TestName: "Value Expr String",
			List:     []sqtables.Expr{sqtables.NewValueExpr(sqtypes.NewSQString("Test STring"))},
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVals:  []sqtypes.Raw{"Test STring"},
			ExpErr:   "",
		},
		{
			TestName: "Col Expr",
			List:     []sqtables.Expr{sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))},
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVals:  []sqtypes.Raw{1},
			ExpErr:   "",
		},
		{
			TestName: "Col Expr not validated",
			List: []sqtables.Expr{
				sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")),
				sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false)),
			},
			profile: profile,
			Tables:  tables,
			rows:    rows,
			ExpVals: []sqtypes.Raw{
				"Test STring",
				12,
			},
			ExpErr:      "Internal Error: Expression list has not been validated before Evaluate",
			UnValidated: true,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testEvalListFunc(row))
	}

}

func testAddFunc(eList *sqtables.ExprList, e sqtables.Expr, hascount bool) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		preLen := eList.Len()
		preStr := eList.String()
		eList.Add(e)
		postStr := eList.String()
		if eList.Len() != preLen+1 {
			t.Errorf("Expression List Len before/after (%d/%d) did not increase by one", preLen, eList.Len())
			return
		}
		if commaStr(preStr, e.String()) != postStr {
			t.Errorf("Expression List String before/after (%s/%s) did match expected", commaStr(preStr, e.String()), eList.String())
			return
		}
		if hascount != eList.HasAggregateFunc() {
			t.Errorf("HasAggregateFunc does not match expected")
			return
		}
	}
}
func checkNil(ex sqtables.Expr) string {
	if ex == nil {
		return "nil"
	}
	return ex.String()
}
func commaStr(a, b string) string {
	if a == "" || b == "" {
		return a + b
	}
	return a + "," + b
}

/* No currently used
func testPopFunc(eList *sqtables.ExprList, ExpExpr sqtables.Expr) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		preLen := eList.Len()
		preStr := eList.String()
		actExpr := eList.Pop()

		if !reflect.DeepEqual(actExpr, ExpExpr) {

			t.Errorf("Expected Expression %q does not match actual Expression %q", checkNil(ExpExpr), checkNil(actExpr))
		}
		if actExpr == nil && preLen == 0 {
			return
		}
		postStr := eList.String()
		if eList.Len()+1 != preLen {
			t.Errorf("Expression List Len before/after (%d/%d) did not increase by one", preLen, eList.Len())
			return
		}
		if preStr != commaStr(postStr, ExpExpr.String()) {
			t.Errorf("Expression List String before/after (%s/%s) did match expected", preStr, commaStr(eList.String(), ExpExpr.String()))
			return
		}
	}
}
*/

func testValidateColsFunc(eList *sqtables.ExprList, ExpErr string, profile *sqprofile.SQProfile, tables sqtables.TableList) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		err := eList.ValidateCols(profile, tables)
		if sqtest.CheckErr(t, err, ExpErr) {
			return
		}

	}
}

func testListEncDecFunc(eList *sqtables.ExprList) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		bin := eList.Encode()
		actList := sqtables.DecodeExprList(bin)

		if !reflect.DeepEqual(eList, actList) {
			t.Error("ExprList does not match encoded/decoded version")
			return
		}

	}
}
func TestEvalListMisc(t *testing.T) {

	var eList *sqtables.ExprList
	eList = sqtables.NewExprList()

	t.Run("Encode/Decode Empty List", testListEncDecFunc(eList))
	ExpExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	//	t.Run("Pop Empty List", testPopFunc(eList, nil))

	t.Run("Add Expr", testAddFunc(eList, ExpExpr, false))

	//	t.Run("Pop 1 Element List", testPopFunc(eList, ExpExpr))

	t.Run("Add 10", testAddFunc(eList, sqtables.NewValueExpr(sqtypes.NewSQInt(10)), false))
	t.Run("Add 11", testAddFunc(eList, sqtables.NewValueExpr(sqtypes.NewSQInt(11)), false))
	t.Run("Add 12", testAddFunc(eList, sqtables.NewValueExpr(sqtypes.NewSQInt(12)), false))
	t.Run("Add Expr 2", testAddFunc(eList, ExpExpr, false))
	t.Run("Encode/Decode List", testListEncDecFunc(eList))

	//	t.Run("Pop 1 Element List 2", testPopFunc(eList, ExpExpr))

	t.Run("Add count", testAddFunc(eList, sqtables.NewFuncExpr(tokens.Count, nil), true))
	//	t.Run("Encode/Decode List Err", testListEncDecFunc(eList))

	t.Run("ExprList from Values", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		vals := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{1, "test", true})
		eList := sqtables.NewExprListFromValues(vals)
		actValues, err := eList.GetValues()
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
			return
		}
		if !reflect.DeepEqual(sqtypes.ValueArray(actValues), vals) {
			t.Error("ExprList does not match Values given")
			return
		}

	})

	errList := sqtables.NewExprList(
		sqtables.NewOpExpr(
			sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
			tokens.Asc,
			sqtables.NewValueExpr(sqtypes.NewSQInt(9)),
		),
	)
	t.Run("ExprList GetValues with Err", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		ExpErr := "Syntax Error: Invalid Int Operator ASC"
		_, err := errList.GetValues()
		if sqtest.CheckErr(t, err, ExpErr) {
			return
		}

	})
	errList = sqtables.NewExprList(
		sqtables.NewOpExpr(
			sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
			tokens.Plus,
			sqtables.NewValueExpr(sqtypes.NewSQInt(9)),
		),
		sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false)),
	)
	t.Run("ExprList GetValues with Column", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		ExpErr := "Syntax Error: Expression did not reduce to a Value"
		_, err := errList.GetValues()
		if sqtest.CheckErr(t, err, ExpErr) {
			return
		}

	})
}

func TestValidateColsExprList(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tableName := "elistValidatetest"
	tab := sqtables.CreateTableDef(tableName,
		[]column.Def{
			column.NewDef("col1", tokens.Int, false),
			column.NewDef("col2", tokens.String, false),
			column.NewDef("col3", tokens.Float, false),
			column.NewDef("col4", tokens.Bool, false),
		},
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	tables := sqtables.NewTableListFromTableDef(profile, tab)
	cols := column.NewListNames([]string{"col1", "col4", "col3", "col2"})
	eList := sqtables.ColsToExpr(cols)

	t.Run("Validate eList", testValidateColsFunc(eList, "", profile, tables))
	eList.Add(sqtables.NewValueExpr(sqtypes.NewSQInt(1)))
	t.Run("Validate eList with ValueExpr", testValidateColsFunc(eList, "", profile, tables))
	eList.Add(sqtables.NewColExpr(column.Ref{ColName: "colx"}))
	t.Run("Validate eList with Error", testValidateColsFunc(eList, "Error: Column \"colx\" not found in Table(s): elistvalidatetest", profile, tables))

}
