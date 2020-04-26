package sqtables_test

import (
	"fmt"
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
		defer sqtest.PanicTestRecovery(t, "")

		_, ok := d.i.(sqtables.Expr)
		if !ok {
			t.Error("Object is not a Expr(ession)")
		}

	}
}

func testLeftFunc(e, ExpExpr sqtables.Expr) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if !reflect.DeepEqual(e.Left(), ExpExpr) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}
func testRightFunc(e, ExpExpr sqtables.Expr) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if !reflect.DeepEqual(e.Right(), ExpExpr) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}
func testSetLeftFunc(a, b sqtables.Expr, ExpPanic string) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, ExpPanic)

		a.SetLeft(b)
		if !reflect.DeepEqual(a.Left(), b) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}
func testSetRightFunc(a, b sqtables.Expr, ExpPanic string) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, ExpPanic)

		a.SetRight(b)
		if !reflect.DeepEqual(a.Right(), b) {
			t.Errorf("Actual Expr does not match Expected Expr")
			return
		}
	}
}
func testStringFunc(e sqtables.Expr, ExpVal string, alias string) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		e.SetAlias(alias)

		if e.String() != ExpVal {
			t.Errorf("Actual value %q does not match Expected value %q", e.String(), ExpVal)
			return
		}
	}
}
func testGetNameFunc(e sqtables.Expr, ExpVal string, alias string) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

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
func testGetColDefFunc(e sqtables.Expr, col sqtables.ColDef, ExpPanic string) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, ExpPanic)

		if !reflect.DeepEqual(e.ColDef(), col) {
			t.Errorf("Actual value %v does not match Expected value %v", e.ColDef(), col)
			return
		}
	}
}
func testColDefsFunc(d ColDefsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		retCols := d.TestExpr.ColDefs(d.Tables...)
		if !reflect.DeepEqual(retCols, d.ExpCols) {
			t.Errorf("Actual value %v does not match Expected value %v", retCols, d.ExpCols)
			return
		}
	}
}
func TestGetLeftExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpExpr  sqtables.Expr
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpExpr: nil},
		{TestName: "ColExpr", TestExpr: cExpr, ExpExpr: nil},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, tokens.Plus, vExpr), ExpExpr: cExpr},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpExpr: vExpr},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.Float, vExpr), ExpExpr: vExpr},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testLeftFunc(row.TestExpr, row.ExpExpr))
	}
}

func TestSetLeftExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpExpr  sqtables.Expr
		ExpPanic string
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpExpr: nil, ExpPanic: "Invalid to SetLeft on a ValueExpr"},
		{TestName: "ColExpr", TestExpr: cExpr, ExpExpr: nil, ExpPanic: "Invalid to SetLeft on a ValueExpr"},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, tokens.Plus, vExpr), ExpExpr: cExpr, ExpPanic: ""},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpExpr: vExpr, ExpPanic: ""},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.Float, vExpr), ExpExpr: vExpr, ExpPanic: ""},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSetLeftFunc(row.TestExpr, row.ExpExpr, row.ExpPanic))
	}
}

func TestGetRightExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpExpr  sqtables.Expr
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpExpr: nil},
		{TestName: "ColExpr", TestExpr: cExpr, ExpExpr: nil},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, tokens.Plus, vExpr), ExpExpr: vExpr},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpExpr: nil},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.Float, vExpr), ExpExpr: nil},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testRightFunc(row.TestExpr, row.ExpExpr))
	}
}

func TestSetRightExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpExpr  sqtables.Expr
		ExpPanic string
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpExpr: nil, ExpPanic: "Invalid to SetRight on a ValueExpr"},
		{TestName: "ColExpr", TestExpr: cExpr, ExpExpr: nil, ExpPanic: "Invalid to SetRight on a ValueExpr"},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, tokens.Plus, vExpr), ExpExpr: cExpr, ExpPanic: ""},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpExpr: nil, ExpPanic: "Invalid to SetRight on a NegateExpr"},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.Float, vExpr), ExpExpr: nil, ExpPanic: "Invalid to SetRight on a FuncExpr"},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSetRightFunc(row.TestExpr, row.ExpExpr, row.ExpPanic))
	}
}

func TestStringExpr(t *testing.T) {
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
			TestExpr: sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int}),
			ExpVal:   "col1",
		},
		{
			TestName: "ColExpr with alias",
			TestExpr: sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int}),
			ExpVal:   "col1 cAlias",
			Alias:    "cAlias",
		},
		{
			TestName: "OpExpr",
			TestExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int}),
				tokens.Plus,
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
			ExpVal: "(col1+1234)",
		},
		{
			TestName: "OpExpr with Alias",
			TestExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int}),
				tokens.Plus,
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
			ExpVal: "(col1+1234) oAlias",
			Alias:  "oAlias",
		},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))), ExpVal: "(-1234)"},
		{TestName: "NegateExpr with Alias", TestExpr: sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))), ExpVal: "(-1234) nAlias", Alias: "nAlias"},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.Float, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))), ExpVal: "FLOAT(1234)"},
		{TestName: "FuncExpr with Alias", TestExpr: sqtables.NewFuncExpr(tokens.Float, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))), ExpVal: "FLOAT(1234) fAlias", Alias: "fAlias"},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testStringFunc(row.TestExpr, row.ExpVal, row.Alias))
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
		{TestName: "ColExpr", TestExpr: sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int}), ExpVal: "col1", Alias: "colAlias"},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int}), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))), ExpVal: "(col1+1)", Alias: "opAlias"},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1))), ExpVal: "(-1)", Alias: "negAlias"},
		{TestName: "FloatExpr", TestExpr: sqtables.NewFuncExpr(tokens.Float, sqtables.NewValueExpr(sqtypes.NewSQInt(1))), ExpVal: "FLOAT(1)", Alias: "funcAlias"},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetNameFunc(row.TestExpr, row.ExpVal, row.Alias))
	}
}

func TestGetColDefExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpCol   sqtables.ColDef
		ExpPanic string
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpCol: sqtables.ColDef{ColName: "1", ColType: tokens.Int}},
		{TestName: "ColExpr", TestExpr: cExpr, ExpCol: sqtables.ColDef{ColName: "col1", ColType: tokens.Int}},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, tokens.Plus, vExpr), ExpCol: sqtables.ColDef{ColName: "(col1+1)", ColType: tokens.Int}},
		{TestName: "CountExpr", TestExpr: sqtables.NewFuncExpr(tokens.Count, nil), ExpCol: sqtables.ColDef{ColName: "COUNT()", ColType: tokens.Count}},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpCol: sqtables.ColDef{ColName: "(-1)", ColType: tokens.Int}},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.Float, vExpr), ExpCol: sqtables.ColDef{ColName: "FLOAT(1)", ColType: tokens.Float}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetColDefFunc(row.TestExpr, row.ExpCol, row.ExpPanic))
	}
}

type ColDefsData struct {
	TestName string
	TestExpr sqtables.Expr
	ExpCols  []sqtables.ColDef
	Tables   []*sqtables.TableDef
	ExpPanic string
}

func TestColDefsExpr(t *testing.T) {
	col1 := sqtables.ColDef{ColName: "col1", ColType: tokens.Int, TableName: "tablea"}
	col2 := sqtables.ColDef{ColName: "col2", ColType: tokens.String, TableName: "tablea"}
	col2b := sqtables.ColDef{ColName: "col1", ColType: tokens.String, TableName: "tableb"}
	col3b := sqtables.ColDef{ColName: "col3", ColType: tokens.String, TableName: "tableb"}
	taba := sqtables.CreateTableDef("tablea", col1, col2)
	tabb := sqtables.CreateTableDef("tableb", col2b, col3b)
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(col1)
	c2bExpr := sqtables.NewColExpr(col2b)
	// data
	data := []ColDefsData{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpCols: nil},
		{TestName: "ColExpr", TestExpr: cExpr, ExpCols: []sqtables.ColDef{col1}, Tables: []*sqtables.TableDef{taba, tabb}},
		{TestName: "ColExpr different table", TestExpr: cExpr, ExpCols: nil, Tables: []*sqtables.TableDef{tabb}},
		{TestName: "ColExpr nil table", TestExpr: cExpr, ExpCols: []sqtables.ColDef{col1}, Tables: nil},
		{TestName: "OpExpr No col", TestExpr: sqtables.NewOpExpr(vExpr, tokens.Plus, vExpr), ExpCols: nil},
		{TestName: "OpExpr left col", TestExpr: sqtables.NewOpExpr(cExpr, tokens.Plus, vExpr), ExpCols: []sqtables.ColDef{col1}},
		{TestName: "OpExpr right col", TestExpr: sqtables.NewOpExpr(vExpr, tokens.Plus, cExpr), ExpCols: []sqtables.ColDef{col1}},
		{TestName: "OpExpr both col", TestExpr: sqtables.NewOpExpr(c2bExpr, tokens.Plus, cExpr), ExpCols: []sqtables.ColDef{col2b, col1}},
		{TestName: "CountExpr", TestExpr: sqtables.NewFuncExpr(tokens.Count, nil), ExpCols: nil},
		{TestName: "NegateExpr no col", TestExpr: sqtables.NewNegateExpr(vExpr), ExpCols: nil},
		{TestName: "NegateExpr with col", TestExpr: sqtables.NewNegateExpr(cExpr), ExpCols: []sqtables.ColDef{col1}, Tables: []*sqtables.TableDef{taba}},
		{TestName: "FuncExpr no col", TestExpr: sqtables.NewFuncExpr(tokens.Float, vExpr), ExpCols: nil},
		{TestName: "FuncExpr with col", TestExpr: sqtables.NewFuncExpr(tokens.Float, cExpr), ExpCols: []sqtables.ColDef{col1}, Tables: []*sqtables.TableDef{taba}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testColDefsFunc(row))
	}
}

func testEvaluateFunc(d EvalData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if !d.NoValidate {
			d.e.ValidateCols(d.profile, d.Tables)
		}

		retVal, err := d.e.Evaluate(d.profile, d.Partial, d.rows...)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if retVal == nil {
			if d.ExpVal != nil {
				t.Errorf("Actual value \"nil\" does not match Expected value %q", d.ExpVal.String())
			}
			return
		}
		if !reflect.DeepEqual(retVal, d.ExpVal) {
			t.Errorf("Actual value %q does not match Expected value %q", retVal.String(), d.ExpVal.String())
			return
		}
	}
}

type EvalData struct {
	TestName   string
	e          sqtables.Expr
	Partial    bool
	profile    *sqprofile.SQProfile
	Tables     *sqtables.TableList
	rows       []sqtables.RowInterface
	ExpVal     sqtypes.Value
	ExpErr     string
	NoValidate bool
}

func TestEvaluateExpr(t *testing.T) {

	profile := sqprofile.CreateSQProfile()
	str := "Create table valueexprtest (col1 int, col2 string, col3 bool)"
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
	row, err := sqtables.CreateRow(profile, 1, tab, []string{"col1", "col2", "col3"}, sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{1, "test1", true}))
	if err != nil {
		t.Error("Unable to setup table")
		return
	}
	rows := []sqtables.RowInterface{row}
	tables := sqtables.NewTableListFromTableDef(profile, tab)
	data := []EvalData{
		{
			TestName: "Value Expr Int",
			e:        sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQInt(1234),
			ExpErr:   "",
		},
		{
			TestName: "Value Expr String",
			e:        sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQString("Test STring"),
			ExpErr:   "",
		},
		{
			TestName:   "Col Expr",
			e:          sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: "valueexprtest"}),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     sqtypes.NewSQInt(1),
			ExpErr:     "",
			NoValidate: true,
		},
		{
			TestName:   "Col Expr Partial",
			e:          sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: "TableX"}),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     nil,
			ExpErr:     "",
			Partial:    true,
			NoValidate: true,
		},
		{
			TestName:   "Col Expr Error",
			e:          sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.String, Idx: 0, TableName: "valueexprtest"}),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     sqtypes.NewSQInt(1),
			ExpErr:     "Error: col1's type of STRING does not match table definition for table valueexprtest",
			NoValidate: true,
		},
		{
			TestName:   "Col Expr Invalid col",
			e:          sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.Int, false)),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     sqtypes.NewSQInt(12),
			ExpErr:     "Error: Column \"colX\" not found in Table(s): valueexprtest",
			NoValidate: true,
		},
		{
			TestName: "OpExpr string add",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString(" Added together"))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQString("Test STring Added together"),
			ExpErr:   "",
		},
		{
			TestName: "OpExpr string minus",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")), tokens.Minus, sqtables.NewValueExpr(sqtypes.NewSQString(" Added together"))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQString("Test STring Added together"),
			ExpErr:   "Syntax Error: Invalid String Operator -",
		},
		{
			TestName: "OpExpr col1 + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.Int, false)), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(2),
			ExpErr:   "",
		}, {
			TestName: "OpExpr 2+col1",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(3),
			ExpErr:   "",
		},
		{
			TestName: "OpExpr colX + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.Int, false)), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(2),
			ExpErr:   "Error: Column \"colX\" not found in Table(s): valueexprtest",
		},
		{
			TestName: "OpExpr 2+colX",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(3),
			ExpErr:   "Error: Column \"colX\" not found in Table(s): valueexprtest",
		},
		{
			TestName: "OpExpr 2+col2 type mismatch",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(sqtables.NewColDef("col2", tokens.String, false))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(3),
			ExpErr:   "Error: Type Mismatch: test1 is not an Int",
		},
		{
			TestName: "OpExpr Deep Tree",
			e: sqtables.NewOpExpr(
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("A")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("A1"))),
						tokens.Plus,
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("B")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("B1"))),
					),
					tokens.Plus,
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("C")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("C1"))),
						tokens.Plus,
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("D")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("D1"))),
					),
				),
				tokens.Plus,
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("E")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("E1"))),
						tokens.Plus,
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("F")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("F1"))),
					),
					tokens.Plus,
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("G")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("G1"))),
						tokens.Plus,
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("H")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("H1"))),
					),
				),
			),
			profile: profile,
			Tables:  tables,
			rows:    rows,
			ExpVal:  sqtypes.NewSQString("AA1BB1CC1DD1EE1FF1GG1HH1"),
			ExpErr:  "",
		},
		{
			TestName: "OpExpr col1+col2 partial",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: "valueexprtest"}),
				tokens.Plus,
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col2", ColType: tokens.Int, Idx: 0, TableName: "tableX"}),
			),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     nil,
			ExpErr:     "",
			Partial:    true,
			NoValidate: true,
		},
		{
			TestName: "OpExpr col2+col1 partial",
			e: sqtables.NewOpExpr(
				sqtables.NewNegateExpr(
					sqtables.NewColExpr(sqtables.ColDef{ColName: "col2", ColType: tokens.Int, Idx: 0, TableName: "tableX"}),
				),
				tokens.Plus,
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: "valueexprtest"}),
			),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     nil,
			ExpErr:     "",
			Partial:    true,
			NoValidate: true,
		},
		{
			TestName: "OpExpr col3 AND col2 partial",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col3", ColType: tokens.Bool, Idx: 0, TableName: "valueexprtest"}),
				tokens.And,
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col2", ColType: tokens.Bool, Idx: 0, TableName: "tableX"}),
			),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     sqtypes.NewSQBool(true),
			ExpErr:     "",
			Partial:    true,
			NoValidate: true,
		},
		{
			TestName: "OpExpr col2 AND col3 partial",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col2", ColType: tokens.Bool, Idx: 0, TableName: "tableX"}),
				tokens.And,
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col3", ColType: tokens.Bool, Idx: 0, TableName: "valueexprtest"}),
			),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     sqtypes.NewSQBool(true),
			ExpErr:     "",
			Partial:    true,
			NoValidate: true,
		},
		{
			TestName: "OpExpr col2 AND col1 partial",
			e: sqtables.NewOpExpr(
				sqtables.NewNegateExpr(
					sqtables.NewColExpr(sqtables.ColDef{ColName: "col2", ColType: tokens.Int, Idx: 0, TableName: "tableX"}),
				),
				tokens.Equal,
				sqtables.NewColExpr(sqtables.ColDef{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: "valueexprtest"}),
			),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     nil,
			ExpErr:     "",
			Partial:    true,
			NoValidate: true,
		},
		{
			TestName: "Count Expr",
			e:        sqtables.NewFuncExpr(tokens.Count, nil),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQNull(),
			ExpErr:   "",
		},
		{
			TestName: "Negate Int",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQInt(-1234),
			ExpErr:   "",
		},
		{
			TestName: "Negate Float",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQFloat(3.14159))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQFloat(-3.14159),
			ExpErr:   "",
		},
		{
			TestName: "Negate String",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test String Neg"))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQInt(-1234),
			ExpErr:   "Syntax Error: STRING values can not be negated",
		},
		{
			TestName: "Double Negate String",
			e:        sqtables.NewNegateExpr(sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test String Neg")))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQInt(-1234),
			ExpErr:   "Syntax Error: STRING values can not be negated",
		},
		{
			TestName: "Negate Bool",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQBool(true))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQInt(-1234),
			ExpErr:   "Syntax Error: BOOL values can not be negated",
		},
		{
			TestName: "Negate Null",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQNull())),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQNull(),
			ExpErr:   "",
		},
		{
			TestName: "Float from Int",
			e:        sqtables.NewFuncExpr(tokens.Float, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			rows:     nil,
			ExpVal:   sqtypes.NewSQFloat(1234),
			ExpErr:   "",
		},
		{
			TestName: "Float from String err",
			e:        sqtables.NewFuncExpr(tokens.Float, sqtables.NewValueExpr(sqtypes.NewSQString("BEZ1234"))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQFloat(1234),
			ExpErr:   "strconv.ParseFloat: parsing \"BEZ1234\": invalid syntax",
		},
		{
			TestName: "Float from Invalid Col",
			e:        sqtables.NewFuncExpr(tokens.Float, sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQFloat(1234),
			ExpErr:   "Error: Column \"colX\" not found in Table(s): valueexprtest",
		},
		{
			TestName: "Invalid Function",
			e:        sqtables.NewFuncExpr(tokens.NilToken, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQFloat(1234),
			ExpErr:   "Syntax Error: \"Invalid\" is not a valid function",
		},
		{
			TestName: "Int from String",
			e:        sqtables.NewFuncExpr(tokens.Int, sqtables.NewValueExpr(sqtypes.NewSQString("1234"))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQInt(1234),
			ExpErr:   "",
		},
		{
			TestName: "Bool from String",
			e:        sqtables.NewFuncExpr(tokens.Bool, sqtables.NewValueExpr(sqtypes.NewSQString("true"))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQBool(true),
			ExpErr:   "",
		},
		{
			TestName: "String from Int",
			e:        sqtables.NewFuncExpr(tokens.String, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQString("1234"),
			ExpErr:   "",
		},
		{
			TestName: "Operator Right with count",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1)), tokens.Plus, sqtables.NewFuncExpr(tokens.Count, nil)),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQNull(),
			ExpErr:   "",
		},
		{
			TestName: "Operator Left with count",
			e:        sqtables.NewOpExpr(sqtables.NewFuncExpr(tokens.Count, nil), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQNull(),
			ExpErr:   "",
		},
		{
			TestName: "Negate with count",
			e:        sqtables.NewNegateExpr(sqtables.NewFuncExpr(tokens.Count, nil)),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQNull(),
			ExpErr:   "",
		},
		{
			TestName: "Int Function with count",
			e:        sqtables.NewFuncExpr(tokens.Int, sqtables.NewFuncExpr(tokens.Count, nil)),
			profile:  profile,
			Tables:   tables,
			rows:     nil,
			ExpVal:   sqtypes.NewSQNull(),
			ExpErr:   "",
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
		defer sqtest.PanicTestRecovery(t, "")

		retVal, err := d.e.Reduce()
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if retVal == nil {
			if d.ExpExpr != "" {
				t.Errorf("Actual value \"nil\" does not match Expected value %q", d.ExpExpr)
			}
			return
		}
		if retVal.String() != d.ExpExpr {
			t.Errorf("Actual value %q does not match Expected value %q", retVal.String(), d.ExpExpr)
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
			e:        sqtables.NewColExpr(sqtables.NewColDef("col2", tokens.String, false)),
			ExpExpr:  "col2",
			ExpErr:   "",
		},
		{
			TestName: "Op Expr",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.NewColDef("col2", tokens.String, false)),
				tokens.Plus,
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			),
			ExpExpr: "(col2+ Test)",
			ExpErr:  "",
		},
		{
			TestName: "Op Expr Left Error",
			e: sqtables.NewOpExpr(
				sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString(" Test"))),
				tokens.Asterix,
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			),
			ExpExpr: "",
			ExpErr:  "Syntax Error: STRING values can not be negated",
		},
		{
			TestName: "Op Expr Right Error",
			e: sqtables.NewOpExpr(
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
				tokens.Asterix,
				sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString(" Test"))),
			),
			ExpExpr: "",
			ExpErr:  "Syntax Error: STRING values can not be negated",
		},
		{
			TestName: "Op Expr Reduced to Value",
			e: sqtables.NewOpExpr(
				sqtables.NewValueExpr(sqtypes.NewSQString("Good")),
				tokens.Plus,
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			),
			ExpExpr: "Good Test",
			ExpErr:  "",
		},
		{
			TestName: "Op Expr Reduced to Value Error",
			e: sqtables.NewOpExpr(
				sqtables.NewValueExpr(sqtypes.NewSQString("Good")),
				tokens.Asterix,
				sqtables.NewValueExpr(sqtypes.NewSQString(" Test")),
			),
			ExpExpr: "Good Test",
			ExpErr:  "Syntax Error: Invalid String Operator *",
		},
		{
			TestName: "Count Expr",
			e:        sqtables.NewFuncExpr(tokens.Count, nil),
			ExpExpr:  "COUNT()",
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
				sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.Int, false)),
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
			ExpErr:  "Syntax Error: STRING values can not be negated",
		},
		{
			TestName: "Function Int Value Expr",
			e: sqtables.NewFuncExpr(
				tokens.String,
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
			ExpExpr: "1234",
			ExpErr:  "",
		},
		{
			TestName: "Function with ColExpr",
			e: sqtables.NewFuncExpr(
				tokens.String,
				sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.Int, false))),
			ExpExpr: "STRING(col1)",
			ExpErr:  "",
		},
		{
			TestName: "Function with Conversion err",
			e: sqtables.NewFuncExpr(
				tokens.Int,
				sqtables.NewValueExpr(sqtypes.NewSQString("BZ1234")),
			),
			ExpExpr: "STRING(col1)",
			ExpErr:  "Error: Unable to Convert \"BZ1234\" to an INT",
		},
		{
			TestName: "Function with Reduce err",
			e: sqtables.NewFuncExpr(
				tokens.String,
				sqtables.NewFuncExpr(
					tokens.Int,
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
		defer sqtest.PanicTestRecovery(t, "")

		err := d.e.ValidateCols(d.profile, d.Tables)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

	}
}

type ValidateData struct {
	TestName string
	e        sqtables.Expr
	profile  *sqprofile.SQProfile
	Tables   *sqtables.TableList
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
	data := []ValidateData{
		{
			TestName: "Value Expr Int",
			e:        sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Value Expr String",
			e:        sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Col Expr",
			e:        sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.NilToken, false)),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Col Expr Invalid col",
			e:        sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.Int, false)),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "Error: Column \"colX\" not found in Table(s): validatecolstest",
		},
		{
			TestName: "OpExpr string add",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString(" Added together"))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "OpExpr string minus",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test STring")), tokens.Minus, sqtables.NewValueExpr(sqtypes.NewSQString(" Added together"))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "OpExpr col1 + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.Int, false)), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		}, {
			TestName: "OpExpr 2+col1",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "OpExpr colX + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.Int, false)), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "Error: Column \"colX\" not found in Table(s): validatecolstest",
		},
		{
			TestName: "OpExpr 2+colX",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "Error: Column \"colX\" not found in Table(s): validatecolstest",
		},
		{
			TestName: "OpExpr 2+col2 type mismatch",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(sqtables.NewColDef("col2", tokens.String, false))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "OpExpr Deep Tree",
			e: sqtables.NewOpExpr(
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("A")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("A1"))),
						tokens.Plus,
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("B")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("B1"))),
					),
					tokens.Plus,
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("C")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("C1"))),
						tokens.Plus,
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("D")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("D1"))),
					),
				),
				tokens.Plus,
				sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("E")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("E1"))),
						tokens.Plus,
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("F")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("F1"))),
					),
					tokens.Plus,
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("G")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("G1"))),
						tokens.Plus,
						sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQString("H")), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQString("H1"))),
					),
				),
			),
			profile: profile,
			Tables:  tables,
			ExpErr:  "",
		},
		{
			TestName: "Count Expr",
			e:        sqtables.NewFuncExpr(tokens.Count, nil),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Negate Int",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Negate Float",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQFloat(3.14159))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Negate String",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test String Neg"))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Double Negate String",
			e:        sqtables.NewNegateExpr(sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQString("Test String Neg")))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Negate Bool",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQBool(true))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Negate Null",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQNull())),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Function with Col Expr",
			e: sqtables.NewFuncExpr(
				tokens.Float,
				sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.NilToken, false)),
			),
			profile: profile,
			Tables:  tables,
			ExpErr:  "",
		},
		{
			TestName: "Function with Col Expr Invalid col",
			e: sqtables.NewFuncExpr(
				tokens.Float,
				sqtables.NewColExpr(sqtables.NewColDef("colX", tokens.Int, false)),
			),
			profile: profile,
			Tables:  tables,
			ExpErr:  "Error: Column \"colX\" not found in Table(s): validatecolstest",
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testValidateFunc(row))
	}

}

func testEncDecFunc(d EncDecData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

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
	ExpPanic string
}

func TestEncDecExpr(t *testing.T) {

	data := []EncDecData{
		{
			TestName: "ValueExpr",
			e:        sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
		},
		{
			TestName: "ColExpr",
			e:        sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.Int, false)),
		},
		{
			TestName: "OpExpr",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(sqtables.NewColDef("col1", tokens.Int, false)),
				tokens.Plus,
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
		},
		{
			TestName: "NegateExpr",
			e:        sqtables.NewNegateExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
		},
		{
			TestName: "CountExpr",
			e:        sqtables.NewFuncExpr(tokens.Count, nil),
			ExpPanic: "FuncExpr Encode not implemented",
		},
		{
			TestName: "FuncExpr",
			e:        sqtables.NewFuncExpr(tokens.Float, sqtables.NewValueExpr(sqtypes.NewSQInt(1234))),
			ExpPanic: "FuncExpr Encode not implemented",
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testEncDecFunc(row))
	}
}

func testDecodeFunc(d DecodeData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

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
	ExpPanic    string
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
			ExpPanic: "",
		},
		{
			TestName: "ValueExpr Error",
			ex:       &sqtables.ValueExpr{},
			ExpExpr:  valueEx,
			bin:      bin,
			ExpPanic: "Found wrong statement type. Expecting IDValueExpr",
		},
		{
			TestName: "ColExpr Error",
			ex:       &sqtables.ColExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: "Found wrong statement type. Expecting ColExpr",
		},
		{
			TestName: "OpExpr Error",
			ex:       &sqtables.OpExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: "Found wrong statement type. Expecting IDOpExpr",
		},
		{
			TestName: "NegateExpr Error",
			ex:       &sqtables.NegateExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: "Found wrong statement type. Expecting IDNegateExpr",
		},
		{
			TestName: "FuncExpr Error",
			ex:       &sqtables.FuncExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: "FuncExpr Decode not implemented",
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
	countBin.Writebyte(sqtables.IDAgregateFunExpr)

	t.Run("CountExpr", func(*testing.T) {
		defer sqtest.PanicTestRecovery(t, "Unexpected Count expression in Decode")

		_ = sqtables.DecodeExpr(countBin)

	})

	t.Run("Unknown Expression", func(*testing.T) {
		defer sqtest.PanicTestRecovery(t, "Unexpected expression type in Decode")

		_ = sqtables.DecodeExpr(bin)

	})
}
