package sqtypes_test

import (
	"fmt"
	"log"
	"os"
	"testing"

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
func testValueType(v sqtypes.Value, expType string) func(*testing.T) {
	return func(t *testing.T) {
		if v.GetType() != expType {
			t.Error(fmt.Sprintf("The expected type of %s does not match actual value of %s", expType, v.GetType()))
		}
	}
}
func testValueToString(v sqtypes.Value, expStr string) func(*testing.T) {
	return func(t *testing.T) {
		if v.ToString() != expStr {
			t.Error(fmt.Sprintf("ToString for type %s produced unexpected results: Actual %q, Expected %q", v.GetType(), v.ToString(), expStr))
		}
	}
}
func testGetLen(v sqtypes.Value, expLen int) func(*testing.T) {
	return func(t *testing.T) {
		if v.GetLen() != expLen {
			t.Error(fmt.Sprintf("The expected Lenght of %d does not match actual value of %d for type %s", expLen, v.GetLen(), v.GetType()))
		}
	}
}

func testEqual(a, b sqtypes.Value, expect bool) func(*testing.T) {
	return func(t *testing.T) {
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
		if expect {
			if !a.GreaterThan(b) {
				t.Error(fmt.Sprintf("%s was expected to be greater than %s", a.ToString(), b.ToString()))
			}
		} else if a.GreaterThan(b) {
			t.Error(fmt.Sprintf("%s was NOT expected to be greater than %s", a.ToString(), b.ToString()))
		}
	}
}
func TestSQInt(t *testing.T) {
	v := sqtypes.NewSQInt(987654321)
	a := sqtypes.NewSQInt(1234)
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
		{"Int test invalid number", *tokens.CreateToken(tokens.Num, "1234AS"), "Syntax Error: \"1234AS\" is not a number", tokens.TypeInt},
		{"String Test", *tokens.CreateToken(tokens.Quote, "This Is a test"), "", tokens.TypeString},
		{"Bool TRUE Test", *tokens.CreateToken(tokens.RWTrue, "TRUE"), "", tokens.TypeBool},
		{"Bool FALSE Test", *tokens.CreateToken(tokens.RWFalse, "FALSE"), "", tokens.TypeBool},
		{"Not A Value Token Test", *tokens.CreateToken(tokens.Ident, "This Is a test"), "Internal Error: [IDENT=This Is a test] is not a valid Value", tokens.TypeString},
	}

	for _, row := range data {
		t.Run(row.Name, testCreateValueFromToken(row.Tkn, row.ErrTxt, row.ExpType))
	}
}

func testCreateValueFromToken(tkn tokens.Token, errTxt string, expType string) func(*testing.T) {
	return func(t *testing.T) {
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
