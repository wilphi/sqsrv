package sqtables_test

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

type InterfaceData struct {
	TestName string
	i        interface{}
}

func TestInterfaces(t *testing.T) {
	data := []InterfaceData{
		{"ValueExpr is an Expr", &sqtables.ValueExpr{}},
		{"ColExpr is an Expr", &sqtables.ColExpr{}},
		{"OpExpr is an Expr", &sqtables.OpExpr{}},
		{"CountExpr is an Expr", &sqtables.CountExpr{}},
		{"NegateExpr is an Expr", &sqtables.NegateExpr{}},
		{"FuncExpr is an Expr", &sqtables.FuncExpr{}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testInterfacesFunc(row))

	}
}

func testInterfacesFunc(d InterfaceData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		_, ok := d.i.(sqtables.Expr)
		if !ok {
			t.Error("Object is not a Expr(ession)")
		}

	}
}

func testLeftFunc(e, ExpExpr sqtables.Expr) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if !reflect.DeepEqual(e.Left(), ExpExpr) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}
func testRightFunc(e, ExpExpr sqtables.Expr) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if !reflect.DeepEqual(e.Right(), ExpExpr) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}
func testSetLeftFunc(a, b sqtables.Expr, expPanic bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if expPanic && r == nil {
				t.Error(t.Name() + " did not panic")
			}
			if !expPanic && r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		a.SetLeft(b)
		if !reflect.DeepEqual(a.Left(), b) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}
func testSetRightFunc(a, b sqtables.Expr, expPanic bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if expPanic && r == nil {
				t.Error(t.Name() + " did not panic")
			}
			if !expPanic && r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		a.SetRight(b)
		if !reflect.DeepEqual(a.Right(), b) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}
func testToStringFunc(e sqtables.Expr, ExpVal string, alias string) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		e.SetAlias(alias)

		if e.ToString() != ExpVal {
			t.Errorf("Actual value %q does not match Expected value %q", e.ToString(), ExpVal)
			return
		}
	}
}
func testGetNameFunc(e sqtables.Expr, ExpVal string, alias string) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if e.Name() != ExpVal {
			t.Errorf("Actual value %q does not match Expected value %q", e.Name(), ExpVal)
			return
		}
		if alias != "" {
			e.SetAlias(alias)
			if e.Name() != alias {
				t.Errorf("Name with alias set: ActualValue %q does not match Expected Value %q", e.Name(), alias)
				return
			}
		}
	}
}
func testGetColDefFunc(e sqtables.Expr, col sqtables.ColDef) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if !reflect.DeepEqual(e.ColDef(), col) {
			t.Errorf("Actual value %v does not match Expected value %v", e.ColDef(), col)
			return
		}
	}
}

func TestGetLeftExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpExpr  sqtables.Expr
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpExpr: nil},
		{TestName: "ColExpr", TestExpr: cExpr, ExpExpr: nil},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, "+", vExpr), ExpExpr: cExpr},
		{TestName: "CountExpr", TestExpr: sqtables.NewCountExpr(), ExpExpr: nil},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpExpr: vExpr},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.TypeFloat, vExpr), ExpExpr: vExpr},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testLeftFunc(row.TestExpr, row.ExpExpr))
	}
}

func TestSetLeftExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpExpr  sqtables.Expr
		ExpPanic bool
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpExpr: nil, ExpPanic: true},
		{TestName: "ColExpr", TestExpr: cExpr, ExpExpr: nil, ExpPanic: true},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, "+", vExpr), ExpExpr: cExpr, ExpPanic: false},
		{TestName: "CountExpr", TestExpr: sqtables.NewCountExpr(), ExpExpr: nil, ExpPanic: true},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpExpr: vExpr, ExpPanic: false},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.TypeFloat, vExpr), ExpExpr: vExpr, ExpPanic: false},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSetLeftFunc(row.TestExpr, row.ExpExpr, row.ExpPanic))
	}
}

func TestGetRightExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpExpr  sqtables.Expr
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpExpr: nil},
		{TestName: "ColExpr", TestExpr: cExpr, ExpExpr: nil},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, "+", vExpr), ExpExpr: vExpr},
		{TestName: "CountExpr", TestExpr: sqtables.NewCountExpr(), ExpExpr: nil},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpExpr: nil},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.TypeFloat, vExpr), ExpExpr: nil},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testRightFunc(row.TestExpr, row.ExpExpr))
	}
}

func TestSetRightExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpExpr  sqtables.Expr
		ExpPanic bool
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpExpr: nil, ExpPanic: true},
		{TestName: "ColExpr", TestExpr: cExpr, ExpExpr: nil, ExpPanic: true},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, "+", vExpr), ExpExpr: cExpr, ExpPanic: false},
		{TestName: "CountExpr", TestExpr: sqtables.NewCountExpr(), ExpExpr: nil, ExpPanic: true},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpExpr: nil, ExpPanic: true},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.TypeFloat, vExpr), ExpExpr: nil, ExpPanic: true},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSetRightFunc(row.TestExpr, row.ExpExpr, row.ExpPanic))
	}
}

func TestToStringExpr(t *testing.T) {
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpVal   string
		Alias    string
	}{
		{
			TestName: "ValueExpr",
			TestExpr: sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			ExpVal:   "1234",
		},
		{
			TestName: "ValueExpr with alias",
			TestExpr: sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			ExpVal:   "1234 vAlias",
			Alias:    "vAlias",
		},
		{
			TestName: "ColExpr",
			TestExpr: sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"}),
			ExpVal:   "col1",
		},
		{
			TestName: "ColExpr with alias",
			TestExpr: sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"}),
			ExpVal:   "col1 cAlias",
			Alias:    "cAlias",
		},
		{
			TestName: "OpExpr",
			TestExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"}),
				"+",
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
			ExpVal: "(col1+1234)",
		},
		{
			TestName: "OpExpr with Alias",
			TestExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"}),
				"+",
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
			ExpVal: "(col1+1234) oAlias",
			Alias:  "oAlias",
		},
		{TestName: "CountExpr", TestExpr: sqtables.NewCountExpr(), ExpVal: "count()"},
		{TestName: "CountExpr with Alias", TestExpr: sqtables.NewCountExpr(), ExpVal: "count() cntAlias", Alias: "cntAlias"},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))), ExpVal: "(-1234)"},
		{TestName: "NegateExpr with Alias", TestExpr: sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))), ExpVal: "(-1234) nAlias", Alias: "nAlias"},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.TypeFloat, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))), ExpVal: "FLOAT(1234)"},
		{TestName: "FuncExpr with Alias", TestExpr: sqtables.NewFuncExpr(tokens.TypeFloat, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))), ExpVal: "FLOAT(1234) fAlias", Alias: "fAlias"},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testToStringFunc(row.TestExpr, row.ExpVal, row.Alias))
	}
}

func TestGetNameExpr(t *testing.T) {
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpVal   string
		Alias    string
	}{
		{TestName: "ValueExpr", TestExpr: sqtables.NewValueExpr(sqtypes.NewSQInt(1)), ExpVal: "1", Alias: "vAlias"},
		{TestName: "ColExpr", TestExpr: sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"}), ExpVal: "col1", Alias: "colAlias"},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"}), "+", sqtables.NewValueExpr(sqtypes.NewSQInt(1))), ExpVal: "(col1+1)", Alias: "opAlias"},
		{TestName: "CountExpr", TestExpr: sqtables.NewCountExpr(), ExpVal: "count()", Alias: "cntAlias"},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1))), ExpVal: "(-1)", Alias: "negAlias"},
		{TestName: "FloatExpr", TestExpr: sqtables.NewFuncExpr(tokens.TypeFloat, sqtables.NewValueExpr(sqtypes.NewSQInt(1))), ExpVal: "FLOAT(1)", Alias: "funcAlias"},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetNameFunc(row.TestExpr, row.ExpVal, row.Alias))
	}
}

func TestGetColDefExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: "INT"})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpCol   sqtables.ColDef
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpCol: sqtables.ColDef{ColName: "1", ColType: "INT"}},
		{TestName: "ColExpr", TestExpr: cExpr, ExpCol: sqtables.ColDef{ColName: "col1", ColType: "INT"}},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, "+", vExpr), ExpCol: sqtables.ColDef{ColName: "(col1+1)", ColType: "INT"}},
		{TestName: "CountExpr", TestExpr: sqtables.NewCountExpr(), ExpCol: sqtables.ColDef{ColName: "count()", ColType: "INT"}},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpCol: sqtables.ColDef{ColName: "(-1)", ColType: "INT"}},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.TypeFloat, vExpr), ExpCol: sqtables.ColDef{ColName: "FLOAT(1)", ColType: "FUNC"}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetColDefFunc(row.TestExpr, row.ExpCol))
	}
}

func testEvaluateFunc(d EvalData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		retVal, err := d.e.Evaluate(d.profile, d.row)
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
		if retVal == nil {
			if d.ExpVal != nil {
				t.Errorf("Actual value \"nil\" does not match Expected value %q", d.ExpVal.ToString())
			}
			return
		}
		if !reflect.DeepEqual(retVal, d.ExpVal) {
			t.Errorf("Actual value %q does not match Expected value %q", retVal.ToString(), d.ExpVal.ToString())
			return
		}
	}
}

type EvalData struct {
	TestName string
	e        sqtables.Expr
	profile  *sqprofile.SQProfile
	row      *sqtables.RowDef
	ExpVal   sqtypes.Value
	ExpErr   string
}

func TestEvaluateExpr(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	str := "Create table valueexprtest (col1 int, col2 string)"
	tableName, _, err := cmd.CreateTable(profile, tokens.Tokenize(str))
	if err != nil {
		t.Error("Unable to setup table")
		return
	}
	str = "Insert into valueexprtest (col1, col2) values (1,\"test1\"),(2,\"test2\")"
	_, _, err = cmd.InsertInto(profile, tokens.Tokenize(str))
	if err != nil {
		t.Error("Unable to setup table")
		return
	}
	tab := sqtables.GetTable(profile, tableName)
	if tab == nil {
		t.Error("Unable to get setup table")
		return
	}
	row := tab.GetRow(profile, 1)
	data := []EvalData{
		{
			TestName: "Value Expr Int",
			e:        sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQInt(1234),
			ExpErr:   "",
		},
		{
			TestName: "Value Expr String",
			e:        sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQString("Test STring"),
			ExpErr:   "",
		},
		{
			TestName: "Col Expr",
			e:        sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false)),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQInt(1),
			ExpErr:   "",
		},
		{
			TestName: "Col Expr Invalid col",
			e:        sqtables.NewColExpr(sqtables.CreateColDef("colX", "INT", false)),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQInt(12),
			ExpErr:   "Error: colX not found in table valueexprtest",
		},
		{
			TestName: "OpExpr string add",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")), "+", sqtables.NewValueExpr(sqtypes.NewSQString(" Added together"))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQString("Test STring Added together"),
			ExpErr:   "",
		},
		{
			TestName: "OpExpr string minus",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")), "-", sqtables.NewValueExpr(sqtypes.NewSQString(" Added together"))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQString("Test STring Added together"),
			ExpErr:   "Syntax Error: Invalid String Operator -",
		},
		{
			TestName: "OpExpr col1 + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false)), "+", sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQInt(2),
			ExpErr:   "",
		}, {
			TestName: "OpExpr 2+col1",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), "+", sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQInt(3),
			ExpErr:   "",
		},
		{
			TestName: "OpExpr colX + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.CreateColDef("colX", "INT", false)), "+", sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQInt(2),
			ExpErr:   "Error: colX not found in table valueexprtest",
		},
		{
			TestName: "OpExpr 2+colX",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), "+", sqtables.NewColExpr(sqtables.CreateColDef("colX", "INT", false))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQInt(3),
			ExpErr:   "Error: colX not found in table valueexprtest",
		},
		{
			TestName: "OpExpr 2+col2 type mismatch",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), "+", sqtables.NewColExpr(sqtables.CreateColDef("col2", "STRING", false))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQInt(3),
			ExpErr:   "Error: Type Mismatch: test1 is not an Int",
		},
		{
			TestName: "OpExpr Deep Tree",
			e: sqtables.NewOpExpr(
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("A")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("A1"))),
						"+",
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("B")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("B1"))),
					),
					"+",
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("C")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("C1"))),
						"+",
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("D")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("D1"))),
					),
				),
				"+",
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("E")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("E1"))),
						"+",
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("F")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("F1"))),
					),
					"+",
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("G")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("G1"))),
						"+",
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("H")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("H1"))),
					),
				),
			),
			profile: profile,
			row:     row,
			ExpVal:  sqtypes.NewSQString("AA1BB1CC1DD1EE1FF1GG1HH1"),
			ExpErr:  "",
		},
		{
			TestName: "Count Expr",
			e:        sqtables.NewCountExpr(),
			profile:  profile,
			row:      row,
			ExpVal:   nil,
			ExpErr:   "",
		},
		{
			TestName: "Negate Int",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQInt(-1234),
			ExpErr:   "",
		},
		{
			TestName: "Negate Float",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQFloat(3.14159))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQFloat(-3.14159),
			ExpErr:   "",
		},
		{
			TestName: "Negate String",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test String Neg"))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQInt(-1234),
			ExpErr:   "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Double Negate String",
			e:        sqtables.NewNegateExpr(sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test String Neg")))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQInt(-1234),
			ExpErr:   "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Negate Bool",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQBool(true))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQInt(-1234),
			ExpErr:   "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Negate Null",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQNull())),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQNull(),
			ExpErr:   "",
		},
		{
			TestName: "Float from Int",
			e:        sqtables.NewFuncExpr(tokens.TypeFloat, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQFloat(1234),
			ExpErr:   "",
		},
		{
			TestName: "Float from String err",
			e:        sqtables.NewFuncExpr(tokens.TypeFloat, sqtables.NewValueExpr(sqtypes.NewSQString("BEZ1234"))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQFloat(1234),
			ExpErr:   "strconv.ParseFloat: parsing \"BEZ1234\": invalid syntax",
		},
		{
			TestName: "Float from Invalid Col",
			e:        sqtables.NewFuncExpr(tokens.TypeFloat, sqtables.NewColExpr(sqtables.CreateColDef("colX", "INT", false))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQFloat(1234),
			ExpErr:   "Error: colX not found in table valueexprtest",
		},
		{
			TestName: "Invalid Function",
			e:        sqtables.NewFuncExpr("NotAFunction", sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			row:      row,
			ExpVal:   sqtypes.NewSQFloat(1234),
			ExpErr:   "Syntax Error: \"NotAFunction\" is not a valid function",
		},
		{
			TestName: "Int from String",
			e:        sqtables.NewFuncExpr(tokens.TypeInt, sqtables.NewValueExpr(sqtypes.NewSQString("1234"))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQInt(1234),
			ExpErr:   "",
		},
		{
			TestName: "Bool from String",
			e:        sqtables.NewFuncExpr(tokens.TypeBool, sqtables.NewValueExpr(sqtypes.NewSQString("true"))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQBool(true),
			ExpErr:   "",
		},
		{
			TestName: "String from Int",
			e:        sqtables.NewFuncExpr(tokens.TypeString, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			row:      nil,
			ExpVal:   sqtypes.NewSQString("1234"),
			ExpErr:   "",
		},
		{
			TestName: "Operator Right with count",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1)), "+", sqtables.NewCountExpr()),
			profile:  profile,
			row:      nil,
			ExpVal:   nil,
			ExpErr:   "Error: Unable to evaluate \"count()\"",
		},
		{
			TestName: "Operator Left with count",
			e:        sqtables.NewOpExpr(sqtables.NewCountExpr(), "+", sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			row:      nil,
			ExpVal:   nil,
			ExpErr:   "Error: Unable to evaluate \"count()\"",
		},
		{
			TestName: "Negate with count",
			e:        sqtables.NewNegateExpr(sqtables.NewCountExpr()),
			profile:  profile,
			row:      nil,
			ExpVal:   nil,
			ExpErr:   "Error: Unable to evaluate \"count()\"",
		},
		{
			TestName: "Int Function with count",
			e:        sqtables.NewFuncExpr(tokens.TypeInt, sqtables.NewCountExpr()),
			profile:  profile,
			row:      nil,
			ExpVal:   nil,
			ExpErr:   "Error: Unable to evaluate \"count()\"",
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testEvaluateFunc(row))
	}

}

/////////////////////////////////////////////////////////////////////////////////////

func testReduceFunc(d ReduceData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		retVal, err := d.e.Reduce()
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
		if retVal == nil {
			if d.ExpExpr != "" {
				t.Errorf("Actual value \"nil\" does not match Expected value %q", d.ExpExpr)
			}
			return
		}
		if retVal.ToString() != d.ExpExpr {
			t.Errorf("Actual value %q does not match Expected value %q", retVal.ToString(), d.ExpExpr)
			return
		}
	}
}

type ReduceData struct {
	TestName string
	e        sqtables.Expr
	ExpExpr  string
	ExpErr   string
}

func TestReduceExpr(t *testing.T) {

	data := []ReduceData{
		{
			TestName: "Value Expr Int",
			e:        sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			ExpExpr:  "1234",
			ExpErr:   "",
		},
		{
			TestName: "Col Expr",
			e:        sqtables.NewColExpr(sqtables.CreateColDef("col2", "STRING", false)),
			ExpExpr:  "col2",
			ExpErr:   "",
		},
		{
			TestName: "Op Expr",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.CreateColDef("col2", "STRING", false)),
				"+",
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			),
			ExpExpr: "(col2+ Test)",
			ExpErr:  "",
		},
		{
			TestName: "Op Expr Left Error",
			e: sqtables.NewOpExpr(
				sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString(" Test"))),
				"*",
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			),
			ExpExpr: "",
			ExpErr:  "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Op Expr Right Error",
			e: sqtables.NewOpExpr(
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
				"*",
				sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString(" Test"))),
			),
			ExpExpr: "",
			ExpErr:  "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Op Expr Reduced to Value",
			e: sqtables.NewOpExpr(
				sqtables.NewValueExpr(sqtypes.NewSQString("Good")),
				"+",
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			),
			ExpExpr: "Good Test",
			ExpErr:  "",
		},
		{
			TestName: "Op Expr Reduced to Value Error",
			e: sqtables.NewOpExpr(
				sqtables.NewValueExpr(sqtypes.NewSQString("Good")),
				"*",
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			),
			ExpExpr: "Good Test",
			ExpErr:  "Syntax Error: Invalid String Operator *",
		},
		{
			TestName: "Count Expr",
			e:        sqtables.NewCountExpr(),
			ExpExpr:  "count()",
			ExpErr:   "",
		},
		{
			TestName: "Negate Float Value Expr",
			e: sqtables.NewNegateExpr(
				sqtables.NewValueExpr(sqtypes.NewSQFloat(1.123)),
			),
			ExpExpr: "-1.123",
			ExpErr:  "",
		},
		{
			TestName: "Negate Int Value Expr",
			e: sqtables.NewNegateExpr(
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
			ExpExpr: "-1234",
			ExpErr:  "",
		},
		{
			TestName: "Negate Column Expr",
			e: sqtables.NewNegateExpr(
				sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false)),
			),
			ExpExpr: "(-col1)",
			ExpErr:  "",
		},
		{
			TestName: "Negate Error",
			e: sqtables.NewNegateExpr(sqtables.NewNegateExpr(
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			)),
			ExpExpr: "",
			ExpErr:  "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Function Int Value Expr",
			e: sqtables.NewFuncExpr(
				tokens.TypeString,
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
			ExpExpr: "1234",
			ExpErr:  "",
		},
		{
			TestName: "Function with ColExpr",
			e: sqtables.NewFuncExpr(
				tokens.TypeString,
				sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false))),
			ExpExpr: "STRING(col1)",
			ExpErr:  "",
		},
		{
			TestName: "Function with Conversion err",
			e: sqtables.NewFuncExpr(
				tokens.TypeInt,
				sqtables.NewValueExpr(sqtypes.NewSQString("BZ1234")),
			),
			ExpExpr: "STRING(col1)",
			ExpErr:  "Error: Unable to Convert \"BZ1234\" to an INT",
		},
		{
			TestName: "Function with Reduce err",
			e: sqtables.NewFuncExpr(
				tokens.TypeString,
				sqtables.NewFuncExpr(
					tokens.TypeInt,
					sqtables.NewValueExpr(sqtypes.NewSQString("BZ1234")),
				),
			),
			ExpExpr: "STRING(col1)",
			ExpErr:  "Error: Unable to Convert \"BZ1234\" to an INT",
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testReduceFunc(row))
	}

}

///////////////////////////////////////////////////////////////////////////////////

func testValidateFunc(d ValidateData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		err := d.e.ValidateCols(d.profile, d.Tab)
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

	}
}

type ValidateData struct {
	TestName string
	e        sqtables.Expr
	profile  *sqprofile.SQProfile
	Tab      *sqtables.TableDef
	ExpErr   string
}

func TestValidateCols(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	str := "Create table validatecolstest (col1 int, col2 string)"
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
	tab := sqtables.GetTable(profile, tableName)
	if tab == nil {
		t.Error("Unable to get setup table")
		return
	}
	data := []ValidateData{
		{
			TestName: "Value Expr Int",
			e:        sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "Value Expr String",
			e:        sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "Col Expr",
			e:        sqtables.NewColExpr(sqtables.CreateColDef("col1", "", false)),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "Col Expr Invalid col",
			e:        sqtables.NewColExpr(sqtables.CreateColDef("colX", "INT", false)),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "Error: Table validatecolstest does not have a column named colX",
		},
		{
			TestName: "OpExpr string add",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")), "+", sqtables.NewValueExpr(sqtypes.NewSQString(" Added together"))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "OpExpr string minus",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")), "-", sqtables.NewValueExpr(sqtypes.NewSQString(" Added together"))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "Syntax Error: Invalid String Operator -",
		},
		{
			TestName: "OpExpr col1 + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false)), "+", sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		}, {
			TestName: "OpExpr 2+col1",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), "+", sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "OpExpr colX + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.CreateColDef("colX", "INT", false)), "+", sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "Error: Table validatecolstest does not have a column named colX",
		},
		{
			TestName: "OpExpr 2+colX",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), "+", sqtables.NewColExpr(sqtables.CreateColDef("colX", "INT", false))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "Error: Table validatecolstest does not have a column named colX",
		},
		{
			TestName: "OpExpr 2+col2 type mismatch",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), "+", sqtables.NewColExpr(sqtables.CreateColDef("col2", "STRING", false))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "Error: Type Mismatch: test1 is not an Int",
		},
		{
			TestName: "OpExpr Deep Tree",
			e: sqtables.NewOpExpr(
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("A")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("A1"))),
						"+",
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("B")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("B1"))),
					),
					"+",
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("C")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("C1"))),
						"+",
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("D")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("D1"))),
					),
				),
				"+",
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("E")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("E1"))),
						"+",
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("F")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("F1"))),
					),
					"+",
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("G")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("G1"))),
						"+",
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("H")), "+", sqtables.NewValueExpr(sqtypes.NewSQString("H1"))),
					),
				),
			),
			profile: profile,
			Tab:     tab,
			ExpErr:  "",
		},
		{
			TestName: "Count Expr",
			e:        sqtables.NewCountExpr(),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "Negate Int",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "Negate Float",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQFloat(3.14159))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "Negate String",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test String Neg"))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Double Negate String",
			e:        sqtables.NewNegateExpr(sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test String Neg")))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Negate Bool",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQBool(true))),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "Syntax Error: Only Int & Float values can be negated",
		},
		{
			TestName: "Negate Null",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQNull())),
			profile:  profile,
			Tab:      tab,
			ExpErr:   "",
		},
		{
			TestName: "Function with Col Expr",
			e: sqtables.NewFuncExpr(
				tokens.TypeFloat,
				sqtables.NewColExpr(sqtables.CreateColDef("col1", "", false)),
			),
			profile: profile,
			Tab:     tab,
			ExpErr:  "",
		},
		{
			TestName: "Function with Col Expr Invalid col",
			e: sqtables.NewFuncExpr(
				tokens.TypeFloat,
				sqtables.NewColExpr(sqtables.CreateColDef("colX", "INT", false)),
			),
			profile: profile,
			Tab:     tab,
			ExpErr:  "Error: Table validatecolstest does not have a column named colX",
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testValidateFunc(row))
	}

}

func testEncDecFunc(d EncDecData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if d.ExpPanic && r == nil {
				t.Error(t.Name() + " did not panic")
			}
			if !d.ExpPanic && r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()

		bin := d.e.Encode()

		actE := sqtables.DecodeExpr(bin)

		if !reflect.DeepEqual(d.e, actE) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}

type EncDecData struct {
	TestName string
	e        sqtables.Expr
	ExpPanic bool
}

func TestEncDecExpr(t *testing.T) {

	data := []EncDecData{
		{
			TestName: "ValueExpr",
			e:        sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
		},
		{
			TestName: "ColExpr",
			e:        sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false)),
		},
		{
			TestName: "OpExpr",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.CreateColDef("col1", "INT", false)),
				"+",
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
		},
		{
			TestName: "NegateExpr",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
		},
		{
			TestName: "CountExpr",
			e:        sqtables.NewCountExpr(),
			ExpPanic: true,
		},
		{
			TestName: "FuncExpr",
			e:        sqtables.NewFuncExpr(tokens.TypeFloat, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			ExpPanic: true,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testEncDecFunc(row))
	}
}

func testDecodeFunc(d DecodeData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if d.ExpPanic && r == nil {
				t.Error(t.Name() + " did not panic")
			}
			if !d.ExpPanic && r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()

		d.ex.Decode(d.bin)

		if !reflect.DeepEqual(d.ex, d.ExpExpr) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}

type DecodeData struct {
	TestName    string
	ex, ExpExpr sqtables.Expr
	bin         *sqbin.Codec
	ExpPanic    bool
}

func TestDecodeExpr(t *testing.T) {
	valueEx := sqtables.NewValueExpr(sqtypes.NewSQInt(1234))
	bin := sqbin.NewCodec(nil)
	bin.Writebyte(1)

	data := []DecodeData{
		{
			TestName: "ValueExpr",
			ex:       &sqtables.ValueExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: false,
		},
		{
			TestName: "ValueExpr Error",
			ex:       &sqtables.ValueExpr{},
			ExpExpr:  valueEx,
			bin:      bin,
			ExpPanic: true,
		},
		{
			TestName: "ColExpr Error",
			ex:       &sqtables.ColExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: true,
		},
		{
			TestName: "OpExpr Error",
			ex:       &sqtables.OpExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: true,
		},
		{
			TestName: "NegateExpr Error",
			ex:       &sqtables.NegateExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: true,
		},
		{
			TestName: "CountExpr Error",
			ex:       &sqtables.CountExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: true,
		},
		{
			TestName: "FuncExpr Error",
			ex:       &sqtables.FuncExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: true,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDecodeFunc(row))
	}
}

func TestFunctionDecodeExpr(t *testing.T) {
	bin := sqbin.NewCodec(nil)
	bin.Writebyte(1)
	countBin := sqbin.NewCodec(nil)
	countBin.Writebyte(sqtables.IDCountExpr)

	t.Run("CountExpr", func(*testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Error(t.Name() + " did not panic")
			}
		}()

		_ = sqtables.DecodeExpr(countBin)

	})

	t.Run("Unknown Expression", func(*testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Error(t.Name() + " did not panic")
			}
		}()

		_ = sqtables.DecodeExpr(bin)

	})
}
