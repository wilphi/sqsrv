package sqtypes_test

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func TestMain(m *testing.M) {
	// setup logging
	logFile, err := os.OpenFile("sqtypes_test.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)

	os.Exit(m.Run())

}

type InterfaceData struct {
	TestName string
	i        interface{}
}

func TestInterfaces(t *testing.T) {

	data := []InterfaceData{
		{"SQInt is a Value", sqtypes.SQInt{}},
		{"SQString is a Value", sqtypes.SQString{}},
		{"SQBool is a Value", sqtypes.SQBool{}},
		{"SQNull is a Value", sqtypes.SQNull{}},
		{"SQFloat is a Value", sqtypes.SQFloat{}},
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
		_, ok := d.i.(sqtypes.Value)
		if !ok {
			t.Error("Object is not a Value")
		}

	}
}

func testValueType(v sqtypes.Value, expType string) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if v.GetType() != expType {
			t.Error(fmt.Sprintf("The expected type of %s does not match actual value of %s", expType, v.GetType()))
		}
	}
}
func testValueToString(v sqtypes.Value, expStr string) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if v.ToString() != expStr {
			t.Error(fmt.Sprintf("ToString for type %s produced unexpected results: Actual %q, Expected %q", v.GetType(), v.ToString(), expStr))
		}
	}
}
func testGetLen(v sqtypes.Value, expLen int) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if v.GetLen() != expLen {
			t.Error(fmt.Sprintf("The expected Lenght of %d does not match actual value of %d for type %s", expLen, v.GetLen(), v.GetType()))
		}
	}
}

func testEqual(a, b sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if expect {
			if !a.Equal(b) {
				t.Error(fmt.Sprintf("The values: %s, %s were expected to be equal but are not", a.ToString(), b.ToString()))
			}
		} else if a.Equal(b) {
			t.Error(fmt.Sprintf("The values: %s, %s were expected to be NOT equal but are equal", a.ToString(), b.ToString()))
		}
	}
}

func testLessThan(a, b sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if expect {
			if !a.LessThan(b) {
				t.Error(fmt.Sprintf("%s was expected to be less than %s", a.ToString(), b.ToString()))
			}
		} else if a.LessThan(b) {
			t.Error(fmt.Sprintf("%s was NOT expected to be less than %s", a.ToString(), b.ToString()))
		}
	}
}

func testGreaterThan(a, b sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if expect {
			if !a.GreaterThan(b) {
				t.Error(fmt.Sprintf("%s was expected to be greater than %s", a.ToString(), b.ToString()))
			}
		} else if a.GreaterThan(b) {
			t.Error(fmt.Sprintf("%s was NOT expected to be greater than %s", a.ToString(), b.ToString()))
		}
	}
}

func testisNull(a sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		if a.IsNull() != expect {
			t.Errorf("IsNull actual %t does not match expected %t", a.IsNull(), expect)
			return
		}
	}
}

func testWriteRead(a sqtypes.Value) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		cdc := sqbin.NewCodec([]byte{})
		a.Write(cdc)
		b := sqtypes.ReadValue(cdc)
		if a.IsNull() && b.IsNull() {
			return
		}
		if !a.Equal(b) {
			t.Error("Write then Read of Value does not match")
			return
		}
	}
}

type MathOpData struct {
	name   string
	a, b   sqtypes.Value
	op     string
	ExpVal sqtypes.Value
	ExpErr string
}

func testMathOp(d MathOpData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		actVal, err := d.a.MathOp(d.op, d.b)
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
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}
		if actVal.IsNull() && d.ExpVal.IsNull() {
			return
		}
		if !actVal.Equal(d.ExpVal) {
			t.Errorf("Actual value %q does not match expected value %q", actVal.ToString(), d.ExpVal.ToString())
		}
	}
}

func TestSQInt(t *testing.T) {
	v := sqtypes.NewSQInt(987654321)
	a := sqtypes.NewSQInt(1234)
	b := sqtypes.NewSQInt(34)
	equalA := sqtypes.NewSQInt(1234)
	notEqualA := sqtypes.NewSQInt(4321)
	t.Run("Type Test", testValueType(v, tokens.TypeInt))
	t.Run("To String Test", testValueToString(v, "987654321"))
	t.Run("GetLen Test", testGetLen(v, sqtypes.SQIntWidth))
	t.Run("Equal Test:equal", testEqual(a, equalA, true))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, true))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, true))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("IsNull", testisNull(a, false))
	t.Run("Write/Read", testWriteRead(a))
	data := []MathOpData{
		{name: "int+int", a: a, b: b, op: "+", ExpVal: sqtypes.NewSQInt(1268), ExpErr: ""},
		{name: "int-int", a: a, b: b, op: "-", ExpVal: sqtypes.NewSQInt(1200), ExpErr: ""},
		{name: "int*int", a: a, b: b, op: "*", ExpVal: sqtypes.NewSQInt(41956), ExpErr: ""},
		{name: "int div int", a: a, b: b, op: "/", ExpVal: sqtypes.NewSQInt(36), ExpErr: ""},
		{name: "int%int", a: a, b: b, op: "%", ExpVal: sqtypes.NewSQInt(10), ExpErr: ""},
		{name: "Invalid operator", a: a, b: b, op: "~", ExpVal: sqtypes.NewSQInt(1268), ExpErr: "Syntax Error: Invalid Operator ~"},
		{name: "Null Value", a: a, b: sqtypes.NewSQNull(), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: ""},
		{name: "Type Mismatch string", a: a, b: sqtypes.NewSQString("test"), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: test is not an Int"},
		{name: "Type Mismatch float", a: a, b: sqtypes.NewSQFloat(1.01), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 1.01 is not an Int"},
	}
	for _, row := range data {
		t.Run(row.name, testMathOp(row))
	}
}

func TestSQString(t *testing.T) {
	v := sqtypes.NewSQString("c test string")
	a := sqtypes.NewSQString("new test string")
	equalA := sqtypes.NewSQString("new test string")
	notEqualA := sqtypes.NewSQString("zz test string")
	t.Run("Type Test", testValueType(v, tokens.TypeString))
	t.Run("To String Test", testValueToString(v, "c test string"))
	t.Run("GetLen Test", testGetLen(v, -sqtypes.SQStringWidth))
	t.Run("Equal Test:equal", testEqual(a, equalA, true))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, true))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, true))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("IsNull", testisNull(a, false))
	t.Run("Write/Read", testWriteRead(a))
	data := []MathOpData{
		{name: "str+str", a: a, b: sqtypes.NewSQString(" !!!"), op: "+", ExpVal: sqtypes.NewSQString("new test string !!!"), ExpErr: ""},
		{name: "str-str", a: a, b: sqtypes.NewSQString(" !!!"), op: "-", ExpVal: sqtypes.NewSQInt(1200), ExpErr: "Syntax Error: Invalid Operator -"},
		{name: "str*str", a: a, b: sqtypes.NewSQString(" !!!"), op: "*", ExpVal: sqtypes.NewSQInt(41956), ExpErr: "Syntax Error: Invalid Operator *"},
		{name: "str div str", a: a, b: sqtypes.NewSQString(" !!!"), op: "/", ExpVal: sqtypes.NewSQInt(36), ExpErr: "Syntax Error: Invalid Operator /"},
		{name: "str%str", a: a, b: sqtypes.NewSQString(" !!!"), op: "%", ExpVal: sqtypes.NewSQInt(10), ExpErr: "Syntax Error: Invalid Operator %"},
		{name: "Invalid operator", a: a, b: sqtypes.NewSQString(" !!!"), op: "~", ExpVal: sqtypes.NewSQInt(1268), ExpErr: "Syntax Error: Invalid Operator ~"},
		{name: "Null Value", a: a, b: sqtypes.NewSQNull(), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: ""},
		{name: "Type Mismatch int", a: a, b: sqtypes.NewSQInt(123), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 123 is not a String"},
		{name: "Type Mismatch float", a: a, b: sqtypes.NewSQFloat(1.01), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 1.01 is not a String"},
	}
	for _, row := range data {
		t.Run(row.name, testMathOp(row))
	}
}
func TestSQBool(t *testing.T) {
	v := sqtypes.NewSQBool(true)
	a := sqtypes.NewSQBool(true)
	equalA := sqtypes.NewSQBool(true)
	notEqualA := sqtypes.NewSQBool(false)
	t.Run("Type Test", testValueType(v, tokens.TypeBool))
	t.Run("To String Test", testValueToString(v, "true"))
	t.Run("GetLen Test", testGetLen(v, sqtypes.SQBoolWidth))
	t.Run("Equal Test:equal", testEqual(a, equalA, true))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, false))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, false))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("IsNull", testisNull(a, false))
	t.Run("Write/Read true", testWriteRead(a))
	t.Run("Write/Read false", testWriteRead(notEqualA))
	t.Run("MathOP", testMathOp(MathOpData{name: "MathOp", a: a, b: notEqualA, op: "+", ExpErr: "Syntax Error: Invalid Operation on type Bool"}))
}
func TestSQNull(t *testing.T) {
	v := sqtypes.NewSQNull()
	a := sqtypes.NewSQNull()
	equalA := sqtypes.NewSQNull()
	notEqualA := sqtypes.NewSQNull()
	t.Run("Type Test", testValueType(v, tokens.Null))
	t.Run("To String Test", testValueToString(v, tokens.Null))
	t.Run("GetLen Test", testGetLen(v, 7))
	t.Run("Equal Test:equal", testEqual(a, equalA, false))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, false))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, false))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("IsNull", testisNull(a, true))
	t.Run("Write/Read", testWriteRead(a))
	t.Run("MathOP", testMathOp(MathOpData{name: "MathOp", a: a, b: notEqualA, op: "+", ExpVal: v, ExpErr: ""}))

}

func TestSQFloat(t *testing.T) {
	v := sqtypes.NewSQFloat(9876543210987654321.0123456789)
	a := sqtypes.NewSQFloat(1234.9876)
	b := sqtypes.NewSQFloat(5.9)
	equalA := sqtypes.NewSQFloat(1234.9876)
	notEqualA := sqtypes.NewSQFloat(4321.0)
	t.Run("Type Test", testValueType(v, tokens.TypeFloat))
	t.Run("To String Test", testValueToString(v, "9.876543210987655E+18"))
	t.Run("To String Test", testValueToString(a, "1234.9876"))
	t.Run("GetLen Test", testGetLen(v, sqtypes.SQFloatWidth))
	t.Run("Equal Test:equal", testEqual(a, equalA, true))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, true))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, true))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("IsNull", testisNull(a, false))
	t.Run("Write/Read", testWriteRead(a))
	data := []MathOpData{
		{name: "float+float", a: a, b: b, op: "+", ExpVal: sqtypes.NewSQFloat(1240.8876), ExpErr: ""},
		{name: "float-float", a: a, b: b, op: "-", ExpVal: sqtypes.NewSQFloat(1229.0875999999998), ExpErr: ""},
		{name: "float*float", a: a, b: b, op: "*", ExpVal: sqtypes.NewSQFloat(7286.42684), ExpErr: ""},
		{name: "float div float", a: a, b: b, op: "/", ExpVal: sqtypes.NewSQFloat(209.3199322033898), ExpErr: ""},
		{name: "float%float", a: a, b: b, op: "%", ExpVal: sqtypes.NewSQInt(10), ExpErr: "Syntax Error: Invalid Operator % on type Float"},
		{name: "Invalid operator", a: a, b: b, op: "~", ExpVal: sqtypes.NewSQInt(1268), ExpErr: "Syntax Error: Invalid Operator ~ on type Float"},
		{name: "Null Value", a: a, b: sqtypes.NewSQNull(), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: ""},
		{name: "Type Mismatch string", a: a, b: sqtypes.NewSQString("test"), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: test is not a Float"},
		{name: "Type Mismatch int", a: a, b: sqtypes.NewSQInt(123), op: "+", ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 123 is not a Float"},
	}
	for _, row := range data {
		t.Run(row.name, testMathOp(row))
	}
}

type tokenValTest struct {
	Name    string
	Tkn     tokens.Token
	ErrTxt  string
	ExpType string
}

func TestCreateValueFromToken(t *testing.T) {
	//Tokentype,
	data := []tokenValTest{
		{"Int test", *tokens.CreateToken(tokens.Num, "1234"), "", tokens.TypeInt},
		{"Negative Int test", *tokens.CreateToken(tokens.Num, "-1234"), "", tokens.TypeInt},
		{"Int test invalid number", *tokens.CreateToken(tokens.Num, "1234AS"), "Syntax Error: \"1234AS\" is not a number", tokens.TypeInt},
		{"Float test", *tokens.CreateToken(tokens.Num, "123.456789"), "", tokens.TypeFloat},
		{"String Test", *tokens.CreateToken(tokens.Quote, "This Is a test"), "", tokens.TypeString},
		{"Bool TRUE Test", *tokens.CreateToken(tokens.RWTrue, "TRUE"), "", tokens.TypeBool},
		{"Bool FALSE Test", *tokens.CreateToken(tokens.RWFalse, "FALSE"), "", tokens.TypeBool},
		{"Null Test", *tokens.CreateToken(tokens.Null, tokens.Null), "", tokens.Null},
		{"Not A Value Token Test", *tokens.CreateToken(tokens.Ident, "This Is a test"), "Internal Error: [IDENT=This Is a test] is not a valid Value", tokens.TypeString},
	}

	for _, row := range data {
		t.Run(row.Name, testCreateValueFromToken(row.Tkn, row.ErrTxt, row.ExpType))
	}
}

func testCreateValueFromToken(tkn tokens.Token, errTxt string, expType string) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		v, err := sqtypes.CreateValueFromToken(tkn)
		if err != nil {
			log.Println(err.Error())
			if errTxt == "" {
				t.Error(fmt.Sprintf("Unexpected Error in test: %s", err.Error()))
				return
			}
			if errTxt != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", errTxt, err.Error()))
				return
			}
			// received expected error
			return
		}
		if err == nil && errTxt != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", errTxt))
			return
		}
		if v.GetType() != expType {
			t.Error(fmt.Sprintf("The expected type of %s does not match actual value of %s", expType, v.GetType()))
		}
	}
}

func TestReadValueFail(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf(t.Name() + " did not panic")
		}
	}()
	cdc := sqbin.NewCodec([]byte{68, 255, 0})

	val := sqtypes.ReadValue(cdc)

	if val == nil {
		t.Error("Unexpected nil Value")
		return
	}
	t.Errorf("Unexpected Value returned from ReadValue: %s", val.ToString())
}
func TestRawValue(t *testing.T) {
	data := []RawValueData{
		{Name: "NULL", ExpPanic: false, Arg: nil, expVal: sqtypes.NewSQNull()},
		{Name: "Int", ExpPanic: false, Arg: 1234, expVal: sqtypes.NewSQInt(1234)},
		{Name: "String", ExpPanic: false, Arg: "Test 1234", expVal: sqtypes.NewSQString("Test 1234")},
		{Name: "Bool true", ExpPanic: false, Arg: true, expVal: sqtypes.NewSQBool(true)},
		{Name: "Bool False", ExpPanic: false, Arg: false, expVal: sqtypes.NewSQBool(false)},
		{Name: "Float", ExpPanic: false, Arg: 123.4, expVal: sqtypes.NewSQFloat(123.4)},
		{Name: "Float64", ExpPanic: false, Arg: float64(123.4), expVal: sqtypes.NewSQFloat(123.4)},
		{Name: "Invalid", ExpPanic: true, Arg: RawValueData{}, expVal: sqtypes.NewSQFloat(123.4)},
	}

	for _, row := range data {
		t.Run(row.Name, testRawValue(row))
	}
}

type RawValueData struct {
	Name     string
	ExpPanic bool
	Arg      sqtypes.Raw
	expVal   sqtypes.Value
}

func testRawValue(d RawValueData) func(*testing.T) {
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
		actVal := sqtypes.RawValue(d.Arg)
		// Null values need special handling to confirm they are equal
		if actVal.IsNull() && d.expVal.IsNull() {
			return
		}
		if !actVal.Equal(d.expVal) {
			t.Errorf("Actual Value %q does not match Expected Value %q", actVal.ToString(), d.expVal.ToString())
			return
		}
	}
}

func TestCreateValuesFromRaw(t *testing.T) {
	defer func() {
		r := recover()
		if r != nil {
			t.Errorf(t.Name() + " panicked unexpectedly")
		}
	}()

	vals := sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, "test1", false, 1.01},
		{2, "test2", true, 2.02},
		{3, "test3", false, 3.03},
	})

	expVals := [][]sqtypes.Value{
		{sqtypes.NewSQInt(1), sqtypes.NewSQString("test1"), sqtypes.NewSQBool(false), sqtypes.NewSQFloat(1.01)},
		{sqtypes.NewSQInt(2), sqtypes.NewSQString("test2"), sqtypes.NewSQBool(true), sqtypes.NewSQFloat(2.02)},
		{sqtypes.NewSQInt(3), sqtypes.NewSQString("test3"), sqtypes.NewSQBool(false), sqtypes.NewSQFloat(3.03)},
	}
	if !reflect.DeepEqual(vals, expVals) {
		t.Error("Actual Values do not match expected values")
		return
	}
}
