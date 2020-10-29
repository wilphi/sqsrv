package sqtypes_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqtest"
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
		defer sqtest.PanicTestRecovery(t, "")

		_, ok := d.i.(sqtypes.Value)
		if !ok {
			t.Error("Object is not a Value")
		}

	}
}

func testValueType(v sqtypes.Value, expType tokens.TokenID) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if v.Type() != expType {
			t.Errorf("The expected type of %s does not match actual value of %s", tokens.IDName(expType), tokens.IDName(v.Type()))
		}
	}
}
func testValueString(v sqtypes.Value, expStr string) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if v.String() != expStr {
			t.Errorf("String for type %s produced unexpected results: Actual %q, Expected %q", tokens.IDName(v.Type()), v.String(), expStr)
		}
	}
}
func testGetLen(v sqtypes.Value, expLen int) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if v.Len() != expLen {
			t.Errorf("The expected Lenght of %d does not match actual value of %d for type %s", expLen, v.Len(), tokens.IDName(v.Type()))
		}
	}
}

func testEqual(a, b sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if expect {
			if !a.Equal(b) {
				t.Errorf("The values: %s, %s were expected to be equal but are not", a.String(), b.String())
			}
		} else if a.Equal(b) {
			t.Errorf("The values: %s, %s were expected to be NOT equal but are equal", a.String(), b.String())
		}
	}
}

func testClone(a sqtypes.Value) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		b := a.Clone()
		if a.IsNull() && b.IsNull() {
			return
		}
		if !a.Equal(b) {
			t.Errorf("The Clone values: %s, %s were expected to be equal but are not", a.String(), b.String())
		}
	}
}

func testLessThan(a, b sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if expect {
			if !a.LessThan(b) {
				t.Errorf("%s was expected to be less than %s", a.String(), b.String())
			}
		} else if a.LessThan(b) {
			t.Errorf("%s was NOT expected to be less than %s", a.String(), b.String())
		}
	}
}

func testGreaterThan(a, b sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if expect {
			if !a.GreaterThan(b) {
				t.Errorf("%s was expected to be greater than %s", a.String(), b.String())
			}
		} else if a.GreaterThan(b) {
			t.Errorf("%s was NOT expected to be greater than %s", a.String(), b.String())
		}
	}
}

func testisNull(a sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if a.IsNull() != expect {
			t.Errorf("IsNull actual %t does not match expected %t", a.IsNull(), expect)
			return
		}
	}
}
func testBoolVal(a sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		b, ok := a.(sqtypes.SQBool)
		if !ok {
			t.Errorf("%T is not a SQBool", a)
			return
		}
		if b.Bool() != expect {
			t.Errorf("Val actual (%t) does not match expected (%t)", b.Bool(), expect)
			return
		}
	}
}
func testNegate(a, expect sqtypes.Value, ExpPanic string) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, ExpPanic)

		b, ok := a.(sqtypes.Negatable)
		if !ok {
			panic(fmt.Sprintf("%T is not Negatable", a))
		}
		n := b.Negate()
		if n.IsNull() && a.IsNull() {
			return
		}
		if !n.Equal(expect) {
			t.Errorf("Negate actual (%s) does not match expected (%s)", n.String(), expect.String())
			return
		}
	}
}
func testWriteRead(a sqtypes.Value) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

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

type OperationData struct {
	name   string
	a, b   sqtypes.Value
	op     tokens.TokenID
	ExpVal sqtypes.Value
	ExpErr string
}

func testOperation(d OperationData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		actVal, err := d.a.Operation(d.op, d.b)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if actVal.IsNull() && d.ExpVal.IsNull() {
			return
		}
		if !actVal.Equal(d.ExpVal) {
			t.Errorf("Actual value %q does not match expected value %q", actVal.String(), d.ExpVal.String())
		}
	}
}

func TestSQInt(t *testing.T) {
	v := sqtypes.NewSQInt(987654321)
	a := sqtypes.NewSQInt(1234)
	negA := sqtypes.NewSQInt(-1234)
	b := sqtypes.NewSQInt(34)
	equalA := sqtypes.NewSQInt(1234)
	notEqualA := sqtypes.NewSQInt(4321)
	nl := sqtypes.NewSQNull()
	t.Run("Type Test", testValueType(v, tokens.Int))
	t.Run("To String Test", testValueString(v, "987654321"))
	t.Run("GetLen Test", testGetLen(v, sqtypes.SQIntWidth))
	t.Run("Equal Test:equal", testEqual(a, equalA, true))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, true))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("LessThan Test:Null", testLessThan(a, nl, true))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, true))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("GreaterThan Test:Null", testGreaterThan(a, nl, false))
	t.Run("IsNull", testisNull(a, false))
	t.Run("Write/Read", testWriteRead(a))
	t.Run("Negate", testNegate(a, negA, ""))
	t.Run("-Negate", testNegate(negA, a, ""))
	t.Run("Clone Test", testClone(a))

	data := []OperationData{
		{name: "int+int", a: a, b: b, op: tokens.Plus, ExpVal: sqtypes.NewSQInt(1268), ExpErr: ""},
		{name: "int-int", a: a, b: b, op: tokens.Minus, ExpVal: sqtypes.NewSQInt(1200), ExpErr: ""},
		{name: "int*int", a: a, b: b, op: tokens.Asterix, ExpVal: sqtypes.NewSQInt(41956), ExpErr: ""},
		{name: "int div int", a: a, b: b, op: tokens.Divide, ExpVal: sqtypes.NewSQInt(36), ExpErr: ""},
		{name: "int%int", a: a, b: b, op: tokens.Modulus, ExpVal: sqtypes.NewSQInt(10), ExpErr: ""},
		{name: "Invalid operator", a: a, b: b, op: tokens.Asc, ExpVal: sqtypes.NewSQInt(1268), ExpErr: "Syntax Error: Invalid Int Operator ASC"},
		{name: "Null Value", a: a, b: sqtypes.NewSQNull(), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: ""},
		{name: "Type Mismatch string", a: a, b: sqtypes.NewSQString("test"), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: test is not an Int"},
		{name: "Type Mismatch float", a: a, b: sqtypes.NewSQFloat(1.01), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 1.01 is not an Int"},
		{name: "int=int : false", a: a, b: notEqualA, op: tokens.Equal, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "int=int : true", a: a, b: equalA, op: tokens.Equal, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "int!=int : true", a: a, b: notEqualA, op: tokens.NotEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "int!=int : false", a: a, b: equalA, op: tokens.NotEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "int<int : true", a: a, b: notEqualA, op: tokens.LessThan, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "int<int : false", a: a, b: equalA, op: tokens.LessThan, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "int>int : true", a: a, b: b, op: tokens.GreaterThan, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "int>int : false", a: a, b: equalA, op: tokens.GreaterThan, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "int<=int : true", a: a, b: notEqualA, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "int<=int : false", a: a, b: b, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "int<=int : Equal true", a: a, b: equalA, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "int>=int : true", a: a, b: b, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "int>=int : false", a: a, b: notEqualA, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "int>=int : true", a: a, b: equalA, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
	}
	for _, row := range data {
		t.Run(row.name, testOperation(row))
	}
}

func TestSQString(t *testing.T) {
	v := sqtypes.NewSQString("c test string")
	a := sqtypes.NewSQString("new test string")
	equalA := sqtypes.NewSQString("new test string")
	notEqualA := sqtypes.NewSQString("zz test string")
	t.Run("Type Test", testValueType(v, tokens.String))
	t.Run("To String Test", testValueString(v, "c test string"))
	t.Run("GetLen Test", testGetLen(v, -sqtypes.SQStringWidth))
	t.Run("Equal Test:equal", testEqual(a, equalA, true))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, true))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("LessThan Test:null", testLessThan(a, sqtypes.NewSQNull(), true))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, true))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("GreaterThan Test:Null", testGreaterThan(a, sqtypes.NewSQNull(), false))
	t.Run("IsNull", testisNull(a, false))
	t.Run("Write/Read", testWriteRead(a))
	t.Run("Negate", testNegate(a, a, "sqtypes.SQString is not Negatable"))
	t.Run("Clone Test", testClone(a))
	data := []OperationData{
		{name: "str+str", a: a, b: sqtypes.NewSQString(" !!!"), op: tokens.Plus, ExpVal: sqtypes.NewSQString("new test string !!!"), ExpErr: ""},
		{name: "str-str", a: a, b: sqtypes.NewSQString(" !!!"), op: tokens.Minus, ExpVal: sqtypes.NewSQInt(1200), ExpErr: "Syntax Error: Invalid String Operator -"},
		{name: "str*str", a: a, b: sqtypes.NewSQString(" !!!"), op: tokens.Asterix, ExpVal: sqtypes.NewSQInt(41956), ExpErr: "Syntax Error: Invalid String Operator *"},
		{name: "str div str", a: a, b: sqtypes.NewSQString(" !!!"), op: tokens.Divide, ExpVal: sqtypes.NewSQInt(36), ExpErr: "Syntax Error: Invalid String Operator /"},
		{name: "str%str", a: a, b: sqtypes.NewSQString(" !!!"), op: tokens.Modulus, ExpVal: sqtypes.NewSQInt(10), ExpErr: "Syntax Error: Invalid String Operator %"},
		{name: "Invalid operator", a: a, b: sqtypes.NewSQString(" !!!"), op: tokens.Asc, ExpVal: sqtypes.NewSQInt(1268), ExpErr: "Syntax Error: Invalid String Operator ASC"},
		{name: "Null Value", a: a, b: sqtypes.NewSQNull(), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: ""},
		{name: "Type Mismatch int", a: a, b: sqtypes.NewSQInt(123), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 123 is not a String"},
		{name: "Type Mismatch float", a: a, b: sqtypes.NewSQFloat(1.01), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 1.01 is not a String"},
		{name: "str=str : false", a: a, b: notEqualA, op: tokens.Equal, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "str=str : true", a: a, b: equalA, op: tokens.Equal, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "str!=str : true", a: a, b: notEqualA, op: tokens.NotEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "str!=str : false", a: a, b: equalA, op: tokens.NotEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "str<str : true", a: a, b: notEqualA, op: tokens.LessThan, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "str<str : false", a: a, b: equalA, op: tokens.LessThan, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "str>str : true", a: a, b: v, op: tokens.GreaterThan, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "str>str : false", a: a, b: equalA, op: tokens.GreaterThan, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "str<=str : true", a: a, b: notEqualA, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "str<=str : false", a: a, b: v, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "str<=str : Equal true", a: a, b: equalA, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "str>=str : true", a: a, b: v, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "str>=str : false", a: a, b: notEqualA, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "str>=str : true", a: a, b: equalA, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
	}
	for _, row := range data {
		t.Run(row.name, testOperation(row))
	}
}
func TestSQBool(t *testing.T) {
	v := sqtypes.NewSQBool(true)
	a := sqtypes.NewSQBool(true)
	b := sqtypes.NewSQBool(false)
	equalA := sqtypes.NewSQBool(true)
	notEqualA := sqtypes.NewSQBool(false)
	t.Run("Type Test", testValueType(v, tokens.Bool))
	t.Run("To String Test", testValueString(v, "true"))
	t.Run("GetLen Test", testGetLen(v, sqtypes.SQBoolWidth))
	t.Run("Equal Test:equal", testEqual(a, equalA, true))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, false))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("LessThan Test:null", testLessThan(a, sqtypes.NewSQNull(), true))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, false))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("GreaterThan Test:Null", testGreaterThan(a, sqtypes.NewSQNull(), false))
	t.Run("IsNull", testisNull(a, false))
	t.Run("Write/Read true", testWriteRead(a))
	t.Run("Write/Read false", testWriteRead(notEqualA))
	t.Run("Val=true", testBoolVal(a, true))
	t.Run("Val=false", testBoolVal(b, false))
	t.Run("Negate", testNegate(a, a, "sqtypes.SQBool is not Negatable"))
	t.Run("Clone Test", testClone(a))
	data := []OperationData{
		{name: "bool+bool", a: a, b: b, op: tokens.Plus, ExpVal: sqtypes.NewSQString("new test string !!!"), ExpErr: "Syntax Error: Invalid Bool Operator +"},
		{name: "bool-bool", a: a, b: b, op: tokens.Minus, ExpVal: sqtypes.NewSQInt(1200), ExpErr: "Syntax Error: Invalid Bool Operator -"},
		{name: "bool*bool", a: a, b: b, op: tokens.Asterix, ExpVal: sqtypes.NewSQInt(41956), ExpErr: "Syntax Error: Invalid Bool Operator *"},
		{name: "bool div bool", a: a, b: b, op: tokens.Divide, ExpVal: sqtypes.NewSQInt(36), ExpErr: "Syntax Error: Invalid Bool Operator /"},
		{name: "bool%bool", a: a, b: b, op: tokens.Modulus, ExpVal: sqtypes.NewSQInt(10), ExpErr: "Syntax Error: Invalid Bool Operator %"},
		{name: "Invalid operator", a: a, b: b, op: tokens.Asc, ExpVal: sqtypes.NewSQInt(1268), ExpErr: "Syntax Error: Invalid Bool Operator ASC"},
		{name: "Null Value", a: a, b: sqtypes.NewSQNull(), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: ""},
		{name: "Type Mismatch int", a: a, b: sqtypes.NewSQInt(123), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 123 is not a Bool"},
		{name: "Type Mismatch float", a: a, b: sqtypes.NewSQFloat(1.01), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 1.01 is not a Bool"},
		{name: "bool=bool : false", a: a, b: notEqualA, op: tokens.Equal, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "bool=bool : true", a: a, b: equalA, op: tokens.Equal, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "bool!=bool : true", a: a, b: notEqualA, op: tokens.NotEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "bool!=bool : false", a: a, b: equalA, op: tokens.NotEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "bool<bool : true", a: a, b: notEqualA, op: tokens.LessThan, ExpVal: sqtypes.NewSQBool(true), ExpErr: "Syntax Error: Invalid Bool Operator <"},
		{name: "bool>bool : true", a: a, b: v, op: tokens.GreaterThan, ExpVal: sqtypes.NewSQBool(true), ExpErr: "Syntax Error: Invalid Bool Operator >"},
		{name: "bool<=bool : true", a: a, b: notEqualA, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: "Syntax Error: Invalid Bool Operator <="},
		{name: "bool>=bool : true", a: a, b: v, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: "Syntax Error: Invalid Bool Operator >="},
		{name: "bool and bool : true", a: a, b: v, op: tokens.And, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "bool and bool : false", a: a, b: b, op: tokens.And, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "bool or bool : true", a: a, b: b, op: tokens.Or, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "bool or bool : false", a: b, b: b, op: tokens.Or, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
	}
	for _, row := range data {
		t.Run(row.name, testOperation(row))
	}
}
func TestSQNull(t *testing.T) {
	v := sqtypes.NewSQNull()
	a := sqtypes.NewSQNull()
	equalA := sqtypes.NewSQNull()
	notEqualA := sqtypes.NewSQNull()
	t.Run("Type Test", testValueType(v, tokens.Null))
	t.Run("To String Test", testValueString(v, tokens.IDName(tokens.Null)))
	t.Run("GetLen Test", testGetLen(v, 7))
	t.Run("Equal Test:equal", testEqual(a, equalA, false))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, false))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("LessThan Test:123", testLessThan(a, sqtypes.NewSQInt(123), false))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, false))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("IsNull", testisNull(a, true))
	t.Run("Write/Read", testWriteRead(a))
	t.Run("Operation", testOperation(OperationData{name: "Operation", a: a, b: notEqualA, op: tokens.Plus, ExpVal: v, ExpErr: ""}))
	t.Run("Negate", testNegate(a, a, ""))
	t.Run("Clone Test", testClone(a))

}

func TestSQFloat(t *testing.T) {
	v := sqtypes.NewSQFloat(9876543210987654321.0123456789)
	a := sqtypes.NewSQFloat(1234.9876)
	negA := sqtypes.NewSQFloat(-1234.9876)
	b := sqtypes.NewSQFloat(5.9)
	equalA := sqtypes.NewSQFloat(1234.9876)
	notEqualA := sqtypes.NewSQFloat(4321.0)
	t.Run("Type Test", testValueType(v, tokens.Float))
	t.Run("To String Test", testValueString(v, "9.876543210987655E+18"))
	t.Run("To String Test", testValueString(a, "1234.9876"))
	t.Run("GetLen Test", testGetLen(v, sqtypes.SQFloatWidth))
	t.Run("Equal Test:equal", testEqual(a, equalA, true))
	t.Run("Equal Test:not equal", testEqual(a, notEqualA, false))
	t.Run("LessThan Test:true", testLessThan(a, notEqualA, true))
	t.Run("LessThan Test:false", testLessThan(notEqualA, a, false))
	t.Run("LessThan Test:equal", testLessThan(a, equalA, false))
	t.Run("LessThan Test:Null", testLessThan(a, sqtypes.NewSQNull(), true))
	t.Run("GreaterThan Test:true", testGreaterThan(notEqualA, a, true))
	t.Run("GreaterThan Test:false", testGreaterThan(a, notEqualA, false))
	t.Run("GreaterThan Test:equal", testGreaterThan(a, equalA, false))
	t.Run("GreaterThan Test:Null", testGreaterThan(a, sqtypes.NewSQNull(), false))
	t.Run("IsNull", testisNull(a, false))
	t.Run("Write/Read", testWriteRead(a))
	t.Run("Negate", testNegate(a, negA, ""))
	t.Run("-Negate", testNegate(negA, a, ""))
	t.Run("Clone Test", testClone(a))
	data := []OperationData{
		{name: "float+float", a: a, b: b, op: tokens.Plus, ExpVal: sqtypes.NewSQFloat(1240.8876), ExpErr: ""},
		{name: "float-float", a: a, b: b, op: tokens.Minus, ExpVal: sqtypes.NewSQFloat(1229.0875999999998), ExpErr: ""},
		{name: "float*float", a: a, b: b, op: tokens.Asterix, ExpVal: sqtypes.NewSQFloat(7286.42684), ExpErr: ""},
		{name: "float div float", a: a, b: b, op: tokens.Divide, ExpVal: sqtypes.NewSQFloat(209.3199322033898), ExpErr: ""},
		{name: "float%float", a: a, b: b, op: tokens.Modulus, ExpVal: sqtypes.NewSQInt(10), ExpErr: "Syntax Error: Invalid Float Operator %"},
		{name: "Invalid operator", a: a, b: b, op: tokens.Asc, ExpVal: sqtypes.NewSQInt(1268), ExpErr: "Syntax Error: Invalid Float Operator ASC"},
		{name: "Null Value", a: a, b: sqtypes.NewSQNull(), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: ""},
		{name: "Type Mismatch string", a: a, b: sqtypes.NewSQString("test"), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: test is not a Float"},
		{name: "Type Mismatch int", a: a, b: sqtypes.NewSQInt(123), op: tokens.Plus, ExpVal: sqtypes.NewSQNull(), ExpErr: "Error: Type Mismatch: 123 is not a Float"},
		{name: "float=float : false", a: a, b: notEqualA, op: tokens.Equal, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "float=float : true", a: a, b: equalA, op: tokens.Equal, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "float!=float : true", a: a, b: notEqualA, op: tokens.NotEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "float!=float : false", a: a, b: equalA, op: tokens.NotEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "float<float : true", a: a, b: notEqualA, op: tokens.LessThan, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "float<float : false", a: a, b: equalA, op: tokens.LessThan, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "float>float : true", a: a, b: b, op: tokens.GreaterThan, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "float>float : false", a: a, b: equalA, op: tokens.GreaterThan, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "float<=float : true", a: a, b: notEqualA, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "float<=float : false", a: a, b: b, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "float<=float : Equal true", a: a, b: equalA, op: tokens.LessThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "float>=float : true", a: a, b: b, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
		{name: "float>=float : false", a: a, b: notEqualA, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(false), ExpErr: ""},
		{name: "float>=float : true", a: a, b: equalA, op: tokens.GreaterThanEqual, ExpVal: sqtypes.NewSQBool(true), ExpErr: ""},
	}
	for _, row := range data {
		t.Run(row.name, testOperation(row))
	}
}

type ConvertData struct {
	TestName string
	V        sqtypes.Value
	NewType  tokens.TokenID
	ExpVal   sqtypes.Raw
	ExpErr   string
}

func testConvertFunc(d ConvertData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		actVal, err := d.V.Convert(d.NewType)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if actVal.IsNull() && d.ExpVal == nil {
			return
		}
		expVal := sqtypes.RawValue(d.ExpVal)
		if !actVal.Equal(expVal) {
			t.Errorf("Actual value %q does not match expected value %v", actVal.String(), d.ExpVal)
		}
	}
}
func TestConvert(t *testing.T) {
	data := []ConvertData{
		{
			TestName: "Int to Int",
			V:        sqtypes.NewSQInt(1234),
			NewType:  tokens.Int,
			ExpVal:   1234,
			ExpErr:   "",
		},
		{
			TestName: "Int to Bool",
			V:        sqtypes.NewSQInt(1234),
			NewType:  tokens.Bool,
			ExpVal:   true,
			ExpErr:   "",
		},
		{
			TestName: "Int to Float",
			V:        sqtypes.NewSQInt(1234),
			NewType:  tokens.Float,
			ExpVal:   1234.0,
			ExpErr:   "",
		},
		{
			TestName: "Int to String",
			V:        sqtypes.NewSQInt(1234),
			NewType:  tokens.String,
			ExpVal:   "1234",
			ExpErr:   "",
		},
		{
			TestName: "Int to Invalid",
			V:        sqtypes.NewSQInt(1234),
			NewType:  tokens.NilToken,
			ExpVal:   "1234",
			ExpErr:   "Error: A value of type INT can not be converted to type Invalid",
		},
		{
			TestName: "Bool to Int True",
			V:        sqtypes.NewSQBool(true),
			NewType:  tokens.Int,
			ExpVal:   1,
			ExpErr:   "",
		},
		{
			TestName: "Bool to Int False",
			V:        sqtypes.NewSQBool(false),
			NewType:  tokens.Int,
			ExpVal:   0,
			ExpErr:   "",
		},
		{
			TestName: "Bool to Bool",
			V:        sqtypes.NewSQBool(true),
			NewType:  tokens.Bool,
			ExpVal:   true,
			ExpErr:   "",
		},
		{
			TestName: "Bool to Float True",
			V:        sqtypes.NewSQBool(true),
			NewType:  tokens.Float,
			ExpVal:   1.0,
			ExpErr:   "",
		},
		{
			TestName: "Bool to Float false",
			V:        sqtypes.NewSQBool(false),
			NewType:  tokens.Float,
			ExpVal:   0.0,
			ExpErr:   "",
		},
		{
			TestName: "Bool to String True",
			V:        sqtypes.NewSQBool(true),
			NewType:  tokens.String,
			ExpVal:   "true",
			ExpErr:   "",
		},
		{
			TestName: "Bool to String False",
			V:        sqtypes.NewSQBool(false),
			NewType:  tokens.String,
			ExpVal:   "false",
			ExpErr:   "",
		},
		{
			TestName: "Bool to Invalid",
			V:        sqtypes.NewSQBool(true),
			NewType:  tokens.NilToken,
			ExpVal:   "1234",
			ExpErr:   "Error: A value of type BOOL can not be converted to type Invalid",
		},
		{
			TestName: "Float to Int",
			V:        sqtypes.NewSQFloat(1234.5678),
			NewType:  tokens.Int,
			ExpVal:   1234,
			ExpErr:   "",
		},
		{
			TestName: "Float to Bool",
			V:        sqtypes.NewSQFloat(1234.5678),
			NewType:  tokens.Bool,
			ExpVal:   true,
			ExpErr:   "",
		},
		{
			TestName: "Float to Float",
			V:        sqtypes.NewSQFloat(1234.5678),
			NewType:  tokens.Float,
			ExpVal:   1234.5678,
			ExpErr:   "",
		},
		{
			TestName: "Float to String",
			V:        sqtypes.NewSQFloat(1234.5678),
			NewType:  tokens.String,
			ExpVal:   "1234.5678",
			ExpErr:   "",
		},
		{
			TestName: "Float to Invalid",
			V:        sqtypes.NewSQFloat(1234.5678),
			NewType:  tokens.NilToken,
			ExpVal:   "1234",
			ExpErr:   "Error: A value of type FLOAT can not be converted to type Invalid",
		},
		{
			TestName: "String to Int",
			V:        sqtypes.NewSQString("1234"),
			NewType:  tokens.Int,
			ExpVal:   1234,
			ExpErr:   "",
		},
		{
			TestName: "String to Int Err",
			V:        sqtypes.NewSQString("1234FAB"),
			NewType:  tokens.Int,
			ExpVal:   1234,
			ExpErr:   "Error: Unable to Convert \"1234FAB\" to an INT",
		},
		{
			TestName: "String to Bool TRUE",
			V:        sqtypes.NewSQString("TruE"),
			NewType:  tokens.Bool,
			ExpVal:   true,
			ExpErr:   "",
		},
		{
			TestName: "String to Bool FALSE",
			V:        sqtypes.NewSQString("false"),
			NewType:  tokens.Bool,
			ExpVal:   false,
			ExpErr:   "",
		},
		{
			TestName: "String to Bool Err",
			V:        sqtypes.NewSQString("Going to Fail"),
			NewType:  tokens.Bool,
			ExpVal:   true,
			ExpErr:   "Error: Unable to convert string to bool",
		},
		{
			TestName: "String to Float",
			V:        sqtypes.NewSQString("1234.5678"),
			NewType:  tokens.Float,
			ExpVal:   1234.5678,
			ExpErr:   "",
		},
		{
			TestName: "String to String",
			V:        sqtypes.NewSQString("DirectCopy"),
			NewType:  tokens.String,
			ExpVal:   "DirectCopy",
			ExpErr:   "",
		},
		{
			TestName: "String to Invalid",
			V:        sqtypes.NewSQString("Invalid"),
			NewType:  tokens.NilToken,
			ExpVal:   "1234",
			ExpErr:   "Error: A value of type STRING can not be converted to type Invalid",
		},
		{
			TestName: "Null Convert",
			V:        sqtypes.NewSQNull(),
			NewType:  tokens.String,
			ExpVal:   nil,
			ExpErr:   "",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testConvertFunc(row))

	}
}

type tokenValTest struct {
	Name    string
	Tkn     tokens.Token
	ExpErr  string
	ExpType tokens.TokenID
}

func TestCreateValueFromToken(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	//Tokentype,
	data := []tokenValTest{
		{"Int test", tokens.NewValueToken(tokens.Num, "1234"), "", tokens.Int},
		{"Negative Int test", tokens.NewValueToken(tokens.Num, "-1234"), "", tokens.Int},
		{"Int test invalid number", tokens.NewValueToken(tokens.Num, "1234AS"), "Syntax Error: \"1234AS\" is not a number", tokens.Int},
		{"Float test", tokens.NewValueToken(tokens.Num, "123.456789"), "", tokens.Float},
		{"String Test", tokens.NewValueToken(tokens.Quote, "This Is a test"), "", tokens.String},
		{"Bool TRUE Test", tokens.GetWordToken(tokens.RWTrue), "", tokens.Bool},
		{"Bool FALSE Test", tokens.GetWordToken(tokens.RWFalse), "", tokens.Bool},
		{"Null Test", tokens.GetWordToken(tokens.Null), "", tokens.Null},
		{"Not A Value Token Test", tokens.NewValueToken(tokens.Ident, "This Is a test"), "Internal Error: \"[IDENT=This Is a test]\" is not a valid Value", tokens.String},
	}

	for _, row := range data {
		t.Run(row.Name, testCreateValueFromToken(row))
	}
}

func testCreateValueFromToken(d tokenValTest) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		v, err := sqtypes.CreateValueFromToken(d.Tkn)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}
		if v.Type() != d.ExpType {
			t.Errorf("The expected type of %s does not match actual value of %s", tokens.IDName(d.ExpType), tokens.IDName(v.Type()))
		}
	}
}

func TestReadValueFail(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "Unknown Value TypeID 255")

	cdc := sqbin.NewCodec([]byte{68, 255, 0})

	val := sqtypes.ReadValue(cdc)

	if val == nil {
		t.Error("Unexpected nil Value")
		return
	}
	t.Errorf("Unexpected Value returned from ReadValue: %s", val.String())
}
func TestRawValue(t *testing.T) {
	data := []RawValueData{
		{Name: "NULL", ExpPanic: "", Arg: nil, expVal: sqtypes.NewSQNull()},
		{Name: "Int", ExpPanic: "", Arg: 1234, expVal: sqtypes.NewSQInt(1234)},
		{Name: "String", ExpPanic: "", Arg: "Test 1234", expVal: sqtypes.NewSQString("Test 1234")},
		{Name: "Bool true", ExpPanic: "", Arg: true, expVal: sqtypes.NewSQBool(true)},
		{Name: "Bool False", ExpPanic: "", Arg: false, expVal: sqtypes.NewSQBool(false)},
		{Name: "Float", ExpPanic: "", Arg: 123.4, expVal: sqtypes.NewSQFloat(123.4)},
		{Name: "Float32", ExpPanic: "", Arg: float32(123.0), expVal: sqtypes.NewSQFloat(123.0)},
		{Name: "Float64", ExpPanic: "", Arg: float64(123.4), expVal: sqtypes.NewSQFloat(123.4)},
		{Name: "Invalid", ExpPanic: "sqtypes_test.RawValueData is not a valid Raw SQ type", Arg: RawValueData{}, expVal: sqtypes.NewSQFloat(123.4)},
	}

	for _, row := range data {
		t.Run(row.Name, testRawValue(row))
	}
}

type RawValueData struct {
	Name     string
	ExpPanic string
	Arg      sqtypes.Raw
	expVal   sqtypes.Value
}

func testRawValue(d RawValueData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		actVal := sqtypes.RawValue(d.Arg)
		// Null values need special handling to confirm they are equal
		if actVal.IsNull() && d.expVal.IsNull() {
			return
		}
		if !actVal.Equal(d.expVal) {
			t.Errorf("Actual Value %q does not match Expected Value %q", actVal.String(), d.expVal.String())
			return
		}
	}
}

func TestCreateValuesFromRaw(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	vals := sqtypes.RawVals{
		{1, "test1", false, 1.01},
		{2, "test2", true, 2.02},
		{3, "test3", false, 3.03},
	}.ValueMatrix()

	expVals := sqtypes.ValueMatrix{
		{sqtypes.NewSQInt(1), sqtypes.NewSQString("test1"), sqtypes.NewSQBool(false), sqtypes.NewSQFloat(1.01)},
		{sqtypes.NewSQInt(2), sqtypes.NewSQString("test2"), sqtypes.NewSQBool(true), sqtypes.NewSQFloat(2.02)},
		{sqtypes.NewSQInt(3), sqtypes.NewSQString("test3"), sqtypes.NewSQBool(false), sqtypes.NewSQFloat(3.03)},
	}

	if !reflect.DeepEqual(vals, expVals) {
		t.Error("Actual Values do not match expected values")
		return
	}
}

type Compare2DData struct {
	TestName     string
	A, B         sqtypes.RawVals
	NameA, NameB string
	DoSort       bool
	ExpRet       string
	SkipRawTest  bool
	CloneA       bool
}

func TestCompare2DValues(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	data := []Compare2DData{
		{
			TestName: "Matching arrays",
			A: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
			},
			B: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
			},
			NameA:  "Actual",
			NameB:  "Expected",
			DoSort: false,
			ExpRet: "",
		},
		{
			TestName: "Clone arrays",
			A: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
			},
			NameA:  "Actual",
			NameB:  "Clone",
			DoSort: false,
			ExpRet: "",
			CloneA: true,
		},
		{
			TestName: "Extra Row",
			A: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
			},
			B: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
				{4, 5, 6, 7},
			},
			NameA:  "Actual",
			NameB:  "Expected",
			DoSort: false,
			ExpRet: "The number of rows does not match! Actual(3) Expected(4)",
		},
		{
			TestName: "Extra col",
			A: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
			},
			B: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5, 9},
				{3, 4, 5, 6},
			},
			NameA:  "Actual",
			NameB:  "Expected",
			DoSort: false,
			ExpRet: "The number of cols does not match! Actual[1]-len=4 Expected[1]-len=5\nActual[1] = [2 3 4 5] Does not match Expected[1] = [2 3 4 5 9]",
		},
		{
			TestName: "Different Value",
			A: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 99, 5},
				{3, 4, 5, 6},
			},
			B: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
			},
			NameA:  "Actual",
			NameB:  "Expected",
			DoSort: false,
			ExpRet: "Actual[1] = [2 3 99 5] Does not match Expected[1] = [2 3 4 5]",
		},
		{
			TestName: "Different Type",
			A: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, "99", 5},
				{3, 4, 5, 6},
			},
			B: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
			},
			NameA:  "Actual",
			NameB:  "Expected",
			DoSort: false,
			ExpRet: "Type Mismatch: Actual[1][2] = string Does not match Expected[1][2] = int",
		},
		{
			TestName: "Sort Rows",
			A: sqtypes.RawVals{
				{1, 2, 3, 4},
				{2, 3, 4, 5},
				{3, 4, 5, 6},
			},
			B: sqtypes.RawVals{
				{2, 3, 4, 5},
				{1, 2, 3, 4},
				{3, 4, 5, 6},
			},
			NameA:       "Actual",
			NameB:       "Expected",
			DoSort:      true,
			ExpRet:      "",
			SkipRawTest: true,
		}}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testCompare2DFunc(row))

	}
}

func testCompare2DFunc(d Compare2DData) func(t *testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")
		var b sqtypes.ValueMatrix
		a := d.A.ValueMatrix()
		if d.CloneA {
			b = a.Clone()
		} else {
			b = d.B.ValueMatrix()
		}
		ret := sqtypes.Compare2DValue(a, b, d.NameA, d.NameB, d.DoSort)

		if ret != d.ExpRet {
			t.Errorf("Actual value %q does not equal Expected value %q", ret, d.ExpRet)
			return
		}

		if !d.CloneA {
			ret = sqtypes.Compare2DRaw(d.A, d.B, d.NameA, d.NameB)
			if ret != d.ExpRet && !d.SkipRawTest {
				t.Errorf("Actual Raw %q does not equal Expected Raw %q", ret, d.ExpRet)
				return
			}
		}

	}
}
