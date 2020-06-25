package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
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
func testGetColDefFunc(e sqtables.Expr, col column.Ref, ExpPanic string) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, ExpPanic)

		if !reflect.DeepEqual(e.ColRef(), col) {
			t.Errorf("Actual value %v does not match Expected value %v", e.ColRef(), col)
			return
		}
	}
}
func testColDefsFunc(d ColDefsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		retCols := d.TestExpr.ColRefs(d.Names...)
		if !reflect.DeepEqual(retCols, d.ExpCols) {
			t.Errorf("Actual value %v does not match Expected value %v", retCols, d.ExpCols)
			return
		}
	}
}
func TestGetLeftExpr(t *testing.T) {
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int})
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
	cExpr := sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int})
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
	cExpr := sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int})
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
	cExpr := sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int})
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
			TestExpr: sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int}),
			ExpVal:   "col1",
		},
		{
			TestName: "ColExpr with alias",
			TestExpr: sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int}),
			ExpVal:   "col1 cAlias",
			Alias:    "cAlias",
		},
		{
			TestName: "ColExpr with same alias",
			TestExpr: sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int}),
			ExpVal:   "col1",
			Alias:    "col1",
		},
		{
			TestName: "OpExpr",
			TestExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int}),
				tokens.Plus,
				sqtables.NewValueExpr(sqtypes.NewSQInt(1234)),
			),
			ExpVal: "(col1+1234)",
		},
		{
			TestName: "OpExpr with Alias",
			TestExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int}),
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
		{TestName: "ColExpr", TestExpr: sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int}), ExpVal: "col1", Alias: "colAlias"},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int}), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))), ExpVal: "(col1+1)", Alias: "opAlias"},
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
	cExpr := sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int})
	data := []struct {
		TestName string
		TestExpr sqtables.Expr
		ExpCol   column.Ref
		ExpPanic string
	}{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpCol: column.Ref{ColName: "1", ColType: tokens.Int}},
		{TestName: "ColExpr", TestExpr: cExpr, ExpCol: column.Ref{ColName: "col1", ColType: tokens.Int}},
		{TestName: "OpExpr", TestExpr: sqtables.NewOpExpr(cExpr, tokens.Plus, vExpr), ExpCol: column.Ref{ColName: "(col1+1)", ColType: tokens.Int}},
		{TestName: "CountExpr", TestExpr: sqtables.NewFuncExpr(tokens.Count, nil), ExpCol: column.Ref{ColName: "COUNT()", ColType: tokens.Count}},
		{TestName: "NegateExpr", TestExpr: sqtables.NewNegateExpr(vExpr), ExpCol: column.Ref{ColName: "(-1)", ColType: tokens.Int}},
		{TestName: "FuncExpr", TestExpr: sqtables.NewFuncExpr(tokens.Float, vExpr), ExpCol: column.Ref{ColName: "FLOAT(1)", ColType: tokens.Float}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGetColDefFunc(row.TestExpr, row.ExpCol, row.ExpPanic))
	}
}

type ColDefsData struct {
	TestName string
	TestExpr sqtables.Expr
	ExpCols  []column.Ref
	Names    []*moniker.Moniker
	ExpPanic string
}

func TestColDefsExpr(t *testing.T) {
	col1 := column.Def{ColName: "col1", ColType: tokens.Int, TableName: "tablea"}
	col1R := col1.Ref()

	col2b := column.Def{ColName: "col1", ColType: tokens.String, TableName: "tableb"}
	col2bR := col2b.Ref()

	namea := moniker.New("tablea", "")
	nameb := moniker.New("tableb", "")
	vExpr := sqtables.NewValueExpr(sqtypes.NewSQInt(1))
	cExpr := sqtables.NewColExpr(col1.Ref())
	c2bExpr := sqtables.NewColExpr(col2b.Ref())
	// data
	data := []ColDefsData{
		{TestName: "ValueExpr", TestExpr: vExpr, ExpCols: nil},
		{TestName: "ColExpr", TestExpr: cExpr, ExpCols: []column.Ref{col1R}, Names: []*moniker.Moniker{namea, nameb}},
		{TestName: "ColExpr different table", TestExpr: cExpr, ExpCols: nil, Names: []*moniker.Moniker{nameb}},
		{TestName: "ColExpr nil table", TestExpr: cExpr, ExpCols: []column.Ref{col1R}, Names: nil}, //Tables: nil},
		{TestName: "OpExpr No col", TestExpr: sqtables.NewOpExpr(vExpr, tokens.Plus, vExpr), ExpCols: nil},
		{TestName: "OpExpr left col", TestExpr: sqtables.NewOpExpr(cExpr, tokens.Plus, vExpr), ExpCols: []column.Ref{col1R}},
		{TestName: "OpExpr right col", TestExpr: sqtables.NewOpExpr(vExpr, tokens.Plus, cExpr), ExpCols: []column.Ref{col1R}},
		{TestName: "OpExpr both col", TestExpr: sqtables.NewOpExpr(c2bExpr, tokens.Plus, cExpr), ExpCols: []column.Ref{col2bR, col1R}},
		{TestName: "CountExpr", TestExpr: sqtables.NewFuncExpr(tokens.Count, nil), ExpCols: nil},
		{TestName: "NegateExpr no col", TestExpr: sqtables.NewNegateExpr(vExpr), ExpCols: nil},
		{TestName: "NegateExpr with col", TestExpr: sqtables.NewNegateExpr(cExpr), ExpCols: []column.Ref{col1R}, Names: []*moniker.Moniker{namea}},
		{TestName: "FuncExpr no col", TestExpr: sqtables.NewFuncExpr(tokens.Float, vExpr), ExpCols: nil},
		{TestName: "FuncExpr with col", TestExpr: sqtables.NewFuncExpr(tokens.Float, cExpr), ExpCols: []column.Ref{col1R}, Names: []*moniker.Moniker{namea}},
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
	tableName := "valueexprtest"
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
			e:          sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: moniker.New("valueexprtest", "")}),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     sqtypes.NewSQInt(1),
			ExpErr:     "",
			NoValidate: true,
		},
		{
			TestName:   "Col Expr Partial",
			e:          sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: moniker.New("TableX", "")}),
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
			e:          sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.String, Idx: 0, TableName: moniker.New("valueexprtest", "")}),
			profile:    profile,
			Tables:     tables,
			rows:       rows,
			ExpVal:     sqtypes.NewSQInt(1),
			ExpErr:     "Error: col1's type of STRING does not match table definition for table valueexprtest",
			NoValidate: true,
		},
		{
			TestName:   "Col Expr Invalid col",
			e:          sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false)),
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
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false)), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(2),
			ExpErr:   "",
		}, {
			TestName: "OpExpr 2+col1",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(3),
			ExpErr:   "",
		},
		{
			TestName: "OpExpr colX + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false)), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(2),
			ExpErr:   "Error: Column \"colX\" not found in Table(s): valueexprtest",
		},
		{
			TestName: "OpExpr 2+colX",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			rows:     rows,
			ExpVal:   sqtypes.NewSQInt(3),
			ExpErr:   "Error: Column \"colX\" not found in Table(s): valueexprtest",
		},
		{
			TestName: "OpExpr 2+col2 type mismatch",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(column.NewRef("col2", tokens.String, false))),
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
				sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: moniker.New("valueexprtest", "")}),
				tokens.Plus,
				sqtables.NewColExpr(column.Ref{ColName: "col2", ColType: tokens.Int, Idx: 0, TableName: moniker.New("tableX", "")}),
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
					sqtables.NewColExpr(column.Ref{ColName: "col2", ColType: tokens.Int, Idx: 0, TableName: moniker.New("tableX", "")}),
				),
				tokens.Plus,
				sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: moniker.New("valueexprtest", "")}),
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
				sqtables.NewColExpr(column.Ref{ColName: "col3", ColType: tokens.Bool, Idx: 0, TableName: moniker.New("valueexprtest", "")}),
				tokens.And,
				sqtables.NewColExpr(column.Ref{ColName: "col2", ColType: tokens.Bool, Idx: 0, TableName: moniker.New("tableX", "")}),
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
				sqtables.NewColExpr(column.Ref{ColName: "col2", ColType: tokens.Bool, Idx: 0, TableName: moniker.New("tableX", "")}),
				tokens.And,
				sqtables.NewColExpr(column.Ref{ColName: "col3", ColType: tokens.Bool, Idx: 0, TableName: moniker.New("valueexprtest", "")}),
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
					sqtables.NewColExpr(column.Ref{ColName: "col2", ColType: tokens.Int, Idx: 0, TableName: moniker.New("tableX", "")}),
				),
				tokens.Equal,
				sqtables.NewColExpr(column.Ref{ColName: "col1", ColType: tokens.Int, Idx: 0, TableName: moniker.New("valueexprtest", "")}),
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
			e:        sqtables.NewFuncExpr(tokens.Float, sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false))),
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
			e:        sqtables.NewColExpr(column.NewRef("col2", tokens.String, false)),
			ExpExpr:  "col2",
			ExpErr:   "",
		},
		{
			TestName: "Op Expr",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(column.NewRef("col2", tokens.String, false)),
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
				sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false)),
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
				sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))),
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

	tableName := "validatecolstest"
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
	_, err = tab.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

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
			e:        sqtables.NewColExpr(column.NewRef("col1", tokens.NilToken, false)),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "Col Expr Invalid col",
			e:        sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false)),
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
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false)), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		}, {
			TestName: "OpExpr 2+col1",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "",
		},
		{
			TestName: "OpExpr colX + 1",
			e:        sqtables.NewOpExpr(sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false)), tokens.Plus, sqtables.NewValueExpr(sqtypes.NewSQInt(1))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "Error: Column \"colX\" not found in Table(s): validatecolstest",
		},
		{
			TestName: "OpExpr 2+colX",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false))),
			profile:  profile,
			Tables:   tables,
			ExpErr:   "Error: Column \"colX\" not found in Table(s): validatecolstest",
		},
		{
			TestName: "OpExpr 2+col2 type mismatch",
			e:        sqtables.NewOpExpr(sqtables.NewValueExpr(sqtypes.NewSQInt(2)), tokens.Plus, sqtables.NewColExpr(column.NewRef("col2", tokens.String, false))),
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
				sqtables.NewColExpr(column.NewRef("col1", tokens.NilToken, false)),
			),
			profile: profile,
			Tables:  tables,
			ExpErr:  "",
		},
		{
			TestName: "Function with Col Expr Invalid col",
			e: sqtables.NewFuncExpr(
				tokens.Float,
				sqtables.NewColExpr(column.NewRef("colX", tokens.Int, false)),
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
			e:        sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false)),
		},
		{
			TestName: "OpExpr",
			e: sqtables.NewOpExpr(
				sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false)),
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
			ExpPanic: "Type marker did not match expected: Actual = 68-TMByte, Expected = 48-TMValueExpr",
		},
		{
			TestName: "ColExpr Error",
			ex:       &sqtables.ColExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: "Type marker did not match expected: Actual = 48-TMValueExpr, Expected = 49-TMColExpr",
		},
		{
			TestName: "OpExpr Error",
			ex:       &sqtables.OpExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: "Type marker did not match expected: Actual = 48-TMValueExpr, Expected = 50-TMOpExpr",
		},
		{
			TestName: "NegateExpr Error",
			ex:       &sqtables.NegateExpr{},
			ExpExpr:  valueEx,
			bin:      valueEx.Encode(),
			ExpPanic: "Type marker did not match expected: Actual = 48-TMValueExpr, Expected = 52-TMNegateExpr",
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
	bin.WriteTypeMarker(sqbin.TypeMarker(1))
	countBin := sqbin.NewCodec(nil)
	countBin.WriteTypeMarker(sqtables.TMAggregateFunExpr)

	t.Run("CountExpr", func(*testing.T) {
		defer sqtest.PanicTestRecovery(t, "Unexpected Count expression in Decode")

		_ = sqtables.DecodeExpr(countBin)

	})

	t.Run("Unknown Expression", func(*testing.T) {
		defer sqtest.PanicTestRecovery(t, "Unexpected expression type in Decode: 1-Unknown Marker")

		_ = sqtables.DecodeExpr(bin)

	})
}

///////////////////////////////////////////////////////////////////////////////////////
//

func testIsAggregateFunc(e sqtables.Expr, ExpVal bool) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		r := e.IsAggregate()

		if r != ExpVal {
			t.Errorf("Actual value %t does not match Expected value %t", r, ExpVal)
			return
		}
	}
}

func TestAggregate(t *testing.T) {
	data := []struct {
		TestName string
		Expr     sqtables.Expr
		ExpVal   bool
	}{
		{TestName: "ValueExpr", Expr: sqtables.NewValueExpr(sqtypes.NewSQInt(1)), ExpVal: false},
		{TestName: "ColExpr", Expr: sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false)), ExpVal: false},
		{TestName: "Negate ValueExpr", Expr: sqtables.NewNegateExpr(sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))), ExpVal: false},
		{TestName: "Negate Aggregate", Expr: sqtables.NewNegateExpr(sqtables.NewFuncExpr(tokens.Count, nil)), ExpVal: true},
		{TestName: "Func Aggregate", Expr: sqtables.NewFuncExpr(tokens.Count, nil), ExpVal: true},
		{TestName: "Func nonAggregate", Expr: sqtables.NewFuncExpr(tokens.String, sqtables.NewValueExpr(sqtypes.NewSQInt(1))), ExpVal: false},
		{TestName: "Func nonAggregate with Aggregate", Expr: sqtables.NewFuncExpr(tokens.String, sqtables.NewFuncExpr(tokens.Count, nil)), ExpVal: true},
		{
			TestName: "OpExpr no Aggregates",
			Expr: sqtables.NewOpExpr(
				sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpVal: false,
		},
		{
			TestName: "OpExpr left only Aggregate",
			Expr: sqtables.NewOpExpr(
				sqtables.NewFuncExpr(tokens.Sum, sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))),
				tokens.Equal,
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
			),
			ExpVal: true,
		},
		{
			TestName: "OpExpr right only Aggregate",
			Expr: sqtables.NewOpExpr(
				sqtables.NewValueExpr(sqtypes.NewSQInt(5)),
				tokens.Equal,
				sqtables.NewFuncExpr(tokens.Sum, sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))),
			),
			ExpVal: true,
		},
		{
			TestName: "OpExpr both Aggregate",
			Expr: sqtables.NewOpExpr(
				sqtables.NewFuncExpr(tokens.Max, sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))),
				tokens.Equal,
				sqtables.NewFuncExpr(tokens.Sum, sqtables.NewColExpr(column.NewRef("col1", tokens.Int, false))),
			),
			ExpVal: true,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testIsAggregateFunc(row.Expr, row.ExpVal))
	}
}

func testProcHavingFunc(d ProcHavingData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		newExpr, _, _ := sqtables.ProcessHaving(d.HavingExpr, []sqtables.FuncExpr{}, 0)

		//fmt.Println("New Expr ", newExpr)
		//fmt.Println(flist)
		//fmt.Println("Count: ", cnt)
		if newExpr.String() != d.ExpExpr.String() {
			t.Errorf("Actual Expression (%s) does not match expected (%s)", newExpr, d.ExpExpr)
		}
	}
}

type ProcHavingData struct {
	TestName   string
	HavingExpr sqtables.Expr
	ExpExpr    sqtables.Expr
	ExpFlist   []sqtables.FuncExpr
}

func TestProcHaving(t *testing.T) {

	data := []ProcHavingData{
		{
			TestName: "Count()>2",
			HavingExpr: sqtables.NewOpExpr(
				sqtables.NewFuncExpr(tokens.Count, nil),
				tokens.GreaterThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(2)),
			),
			ExpExpr: sqtables.NewOpExpr(
				sqtables.NewColExpr(column.Ref{ColName: " Hidden_COUNT()", Idx: 0}),
				tokens.GreaterThan,
				sqtables.NewValueExpr(sqtypes.NewSQInt(2)),
			),
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testProcHavingFunc(row))
	}
}
