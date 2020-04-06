package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
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
	Tables      *sqtables.TableList
	rows        []sqtables.RowInterface
	ExpVals     []sqtypes.Raw
	ExpErr      string
	UnValidated bool
}

func testEvalListFunc(d EvalListData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		eList := sqtables.NewExprList(d.List...)
		if !d.UnValidated {
			err := eList.ValidateCols(d.profile, d.Tables)
			if err != nil {
				t.Errorf("Unable to validate cols: %s", err.Error())
			}
		}
		retVals, err := eList.Evaluate(d.profile, sqtables.EvalFull, d.rows...)

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
		if retVals == nil {
			if d.ExpVals != nil {
				t.Errorf("Actual values \"nil\" does not match Expected values %q", d.ExpVals)
			}
			return
		}
		evals := sqtypes.CreateValueArrayFromRaw(d.ExpVals)
		if !reflect.DeepEqual(retVals, evals) {
			t.Errorf("Actual values %q does not match Expected values %q", retVals, evals)
			return
		}
	}
}
func TestEvalListExpr(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	str := "Create table elisttest (col1 int, col2 string)"
	tableName, _, err := cmd.CreateTable(profile, tokens.Tokenize(str))
	if err != nil {
		t.Error("Unable to setup table")
		return
	}
	str = "Insert into " + tableName + " (col1, col2) values (1,\"test1\"),(2,\"test2\")"
	_, _, err = cmd.InsertInto(profile, tokens.Tokenize(str))
	if err != nil {
		t.Error("Unable to setup table")
		return
	}
	tab, err := sqtables.GetTable(profile, tableName)
	if err != nil {
		t.Error(err)
		return
	}

	if tab == nil {
		t.Error("Unable to get setup table")
		return
	}
	row := tab.GetRow(profile, 1)
	rows := []sqtables.RowInterface{row}
	tables := sqtables.NewTableListFromTableDef(profile, tab)
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
			List:     []sqtables.Expr{sqtables.NewColExpr(sqtables.NewColDef("col1", "INT", false))},
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
				sqtables.NewColExpr(sqtables.NewColDef("colX", "INT", false)),
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
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		preLen := eList.Len()
		preStr := eList.ToString()
		eList.Add(e)
		postStr := eList.ToString()
		if eList.Len() != preLen+1 {
			t.Errorf("Expression List Len before/after (%d/%d) did not increase by one", preLen, eList.Len())
			return
		}
		if commaStr(preStr, e.ToString()) != postStr {
			t.Errorf("Expression List String before/after (%s/%s) did match expected", commaStr(preStr, e.ToString()), eList.ToString())
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
	return ex.ToString()
}
func commaStr(a, b string) string {
	if a == "" || b == "" {
		return a + b
	}
	return a + "," + b
}
func testPopFunc(eList *sqtables.ExprList, ExpExpr sqtables.Expr) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		preLen := eList.Len()
		preStr := eList.ToString()
		actExpr := eList.Pop()

		if !reflect.DeepEqual(actExpr, ExpExpr) {

			t.Errorf("Expected Expression %q does not match actual Expression %q", checkNil(ExpExpr), checkNil(actExpr))
		}
		if actExpr == nil && preLen == 0 {
			return
		}
		postStr := eList.ToString()
		if eList.Len()+1 != preLen {
			t.Errorf("Expression List Len before/after (%d/%d) did not increase by one", preLen, eList.Len())
			return
		}
		if preStr != commaStr(postStr, ExpExpr.ToString()) {
			t.Errorf("Expression List String before/after (%s/%s) did match expected", preStr, commaStr(eList.ToString(), ExpExpr.ToString()))
			return
		}
	}
}

func testValidateColsFunc(eList *sqtables.ExprList, ExpErr string, profile *sqprofile.SQProfile, tables *sqtables.TableList) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		err := eList.ValidateCols(profile, tables)
		if err != nil {
			log.Println(err.Error())
			if ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", ExpErr, err.Error())
				return
			}
			return
		}

	}
}

func testListEncDecFunc(eList *sqtables.ExprList) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
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
	t.Run("Pop Empty List", testPopFunc(eList, nil))

	t.Run("Add Expr", testAddFunc(eList, ExpExpr, false))

	t.Run("Pop 1 Element List", testPopFunc(eList, ExpExpr))

	t.Run("Add 10", testAddFunc(eList, sqtables.NewValueExpr(sqtypes.NewSQInt(10)), false))
	t.Run("Add 11", testAddFunc(eList, sqtables.NewValueExpr(sqtypes.NewSQInt(11)), false))
	t.Run("Add 12", testAddFunc(eList, sqtables.NewValueExpr(sqtypes.NewSQInt(12)), false))
	t.Run("Add Expr 2", testAddFunc(eList, ExpExpr, false))
	t.Run("Encode/Decode List", testListEncDecFunc(eList))

	t.Run("Pop 1 Element List 2", testPopFunc(eList, ExpExpr))

	t.Run("Add count", testAddFunc(eList, sqtables.NewFuncExpr(tokens.Count, nil), true))
	//	t.Run("Encode/Decode List Err", testListEncDecFunc(eList))

	t.Run("ExprList from Values", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		vals := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{1, "test", true})
		eList := sqtables.NewExprListFromValues(vals)
		actValues, err := eList.GetValues()
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
			return
		}
		if !reflect.DeepEqual(actValues, vals) {
			t.Error("ExprList does not match Values given")
			return
		}

	})

	errList := sqtables.NewExprList(
		sqtables.NewOpExpr(
			sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
			"~",
			sqtables.NewValueExpr(sqtypes.NewSQInt(9)),
		),
	)
	t.Run("ExprList GetValues with Err", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		ExpErr := "Syntax Error: Invalid Int Operator ~"
		_, err := errList.GetValues()
		if err != nil {
			log.Println(err.Error())
			if ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", ExpErr, err.Error())
				return
			}
			return
		}
		if err == nil && ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", ExpErr)
			return
		}
	})
	errList = sqtables.NewExprList(
		sqtables.NewOpExpr(
			sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
			"+",
			sqtables.NewValueExpr(sqtypes.NewSQInt(9)),
		),
		sqtables.NewColExpr(sqtables.NewColDef("col1", "INT", false)),
	)
	t.Run("ExprList GetValues with Column", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		ExpErr := "Syntax Error: Expression did not reduce to a Value"
		_, err := errList.GetValues()
		if err != nil {
			log.Println(err.Error())
			if ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", ExpErr, err.Error())
				return
			}
			return
		}
		if err == nil && ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", ExpErr)
			return
		}
	})
}

func TestValidateColsExprList(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	str := "Create table elistValidatetest (col1 int, col2 string, col3 float, col4 bool)"
	tableName, _, err := cmd.CreateTable(profile, tokens.Tokenize(str))
	if err != nil {
		t.Error("Unable to setup table")
		return
	}

	tab, err := sqtables.GetTable(profile, tableName)
	if err != nil {
		t.Error(err)
		return
	}

	if tab == nil {
		t.Error("Unable to get setup table")
		return
	}
	tables := sqtables.NewTableListFromTableDef(profile, tab)
	cols := sqtables.NewColListNames([]string{"col1", "col4", "col3", "col2"})
	eList := sqtables.ColsToExpr(cols)

	t.Run("Validate eList", testValidateColsFunc(eList, "", profile, tables))
	eList.Add(sqtables.NewValueExpr(sqtypes.NewSQInt(1)))
	t.Run("Validate eList with ValueExpr", testValidateColsFunc(eList, "", profile, tables))
	eList.Add(sqtables.NewColExpr(sqtables.ColDef{ColName: "colx"}))
	t.Run("Validate eList with Error", testValidateColsFunc(eList, "Error: Column \"colx\" not found in Table(s): elistvalidatetest", profile, tables))

}
