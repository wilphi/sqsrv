package sqtables_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func testValueType(v sqtypes.Value, expType string) func(*testing.T) {
	return func(t *testing.T) {
		if v.Type() != expType {
			t.Errorf("The expected type of %s does not match actual value of %s", expType, v.Type())
		}
	}
}
func testGetLeft(c sqtables.Condition, result sqtables.Condition) func(*testing.T) {
	return func(t *testing.T) {
		if c.GetLeft() != result {
			expRet := ""
			ret := ""
			if c.GetLeft() == nil {
				ret = "(nil)"
			} else {
				ret = c.GetLeft().ToString()
			}
			if result == nil {
				expRet = "(nil)"
			} else {
				expRet = result.ToString()
			}
			t.Errorf("The expected return %q from GetLeft() does not match actual value %q", expRet, ret)
		}
	}
}

func testGetRight(c sqtables.Condition, result sqtables.Condition) func(*testing.T) {
	return func(t *testing.T) {
		if c.GetRight() != result {
			expRet := ""
			ret := ""
			if c.GetRight() == nil {
				ret = "(nil)"
			} else {
				ret = c.GetRight().ToString()
			}
			if result == nil {
				expRet = "(nil)"
			} else {
				expRet = result.ToString()
			}
			t.Errorf("The expected return %q from GetRight() does not match actual value %q", expRet, ret)
		}
	}
}

/*
// Will need to expand once SetLeft is implemented on other conditions
func testSetLeft(c sqtables.Condition, newCond sqtables.Condition, expectPanic bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if expectPanic && r == nil {
				t.Error("SetLeft() did not panic")
			}
			if !expectPanic && r != nil {
				t.Errorf("SetLeft() panicked unexpectedly")
			}
		}()
		c.SetLeft(newCond)

	}
}

// Will need to expand once SetLeft is implemented on other conditions
func testSetRight(c sqtables.Condition, newCond sqtables.Condition, expectPanic bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if expectPanic && r == nil {
				t.Error("SetLeft() did not panic")
			}
			if !expectPanic && r != nil {
				t.Error("SetLeft() panicked unexpectedly")
			}
		}()
		c.SetRight(newCond)

	}
}
*/

func testToString(c sqtables.Condition, result string) func(*testing.T) {
	return func(t *testing.T) {
		if c.ToString() != result {
			t.Errorf("ToString() =%q, does not match expected result %q", c.ToString(), result)
		}
	}
}

func testEvaluate(profile *sqprofile.SQProfile, c sqtables.Condition, row *sqtables.RowDef, errTxt string, expect bool) func(*testing.T) {
	return func(t *testing.T) {
		ok, err := c.Evaluate(profile, row)
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
			return
		}
		if err == nil && errTxt != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", errTxt))
			return
		}
		if ok != expect {
			t.Errorf("%q Evaluated to %t, expected value was %t", c.ToString(), ok, expect)
		}
	}
}
func TestCompareValCond(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	colInt := sqtables.CreateColDef("testCol", tokens.TypeInt, false)
	cond := sqtables.NewCVCond(colInt, "=", sqtypes.NewSQInt(5))
	cond2 := sqtables.NewCVCond(colInt, "<", sqtypes.NewSQInt(7))
	cond3 := sqtables.NewCVCond(colInt, ">", sqtypes.NewSQInt(7))
	t.Run("GetLeft()", testGetLeft(cond, nil))
	t.Run("GetRight()", testGetRight(cond, nil))
	//	t.Run("SetLeft()", testSetLeft(cond, cond2, true))
	//	t.Run("SetRight()", testSetRight(cond, cond2, true))
	t.Run("ToString()", testToString(cond, "testCol = 5"))

	tableD := sqtables.CreateTableDef("test", colInt)
	row, err := sqtables.CreateRow(profile, 1, tableD, []string{"testCol"}, []sqtypes.Value{sqtypes.NewSQInt(5)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	row2, err := sqtables.CreateRow(profile, 2, tableD, []string{"testCol"}, []sqtypes.Value{sqtypes.NewSQInt(9)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	var tests = []struct {
		Name   string
		Cond   sqtables.Condition
		r      *sqtables.RowDef
		ErrTxt string
		Expect bool
	}{
		{"Evaluate() 5 equal 5", cond, row, "", true},
		{"Evaluate() 9 equal 5", cond, row2, "", false},
		{"Evaluate() 5 lessthan 7", cond2, row, "", true},
		{"Evaluate() 9 lessthan 7", cond2, row2, "", false},
		{"Evaluate() 5 Greaterthan 7", cond3, row, "", false},
		{"Evaluate() 9 Greaterthan 7", cond3, row2, "", true},
		{"Evaluate() invalid col", sqtables.NewCVCond(sqtables.CreateColDef("testCol2", tokens.TypeInt, false), "=", sqtypes.NewSQInt(5)), row, "Error: testCol2 not found in table test", true},
		{"Evaluate() Type Mismatch", sqtables.NewCVCond(colInt, "=", sqtypes.NewSQString("57")), row, "Error: Type Mismatch in Where clause expression: testCol(INT) = 57(STRING)", true},
		{"Evaluate() Operator not implemented", sqtables.NewCVCond(colInt, "~", sqtypes.NewSQInt(5)), row, "Internal Error: Operator ~ is not implemented", true},
	}

	for _, tRow := range tests {

		t.Run(tRow.Name, testEvaluate(profile, tRow.Cond, tRow.r, tRow.ErrTxt, tRow.Expect))
	}

}

func TestLogicCondAND(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	colInt := sqtables.CreateColDef("testCol", tokens.TypeInt, false)
	cond := sqtables.NewCVCond(colInt, "=", sqtypes.NewSQInt(5))
	cond2 := sqtables.NewCVCond(colInt, "<", sqtypes.NewSQInt(7))
	//	cond3 := sqtables.NewCVCond(colInt, ">", sqtypes.NewSQInt(7))

	condAnd := sqtables.NewANDCondition(cond, cond2)
	t.Run("GetLeft()", testGetLeft(condAnd, cond))
	t.Run("GetRight()", testGetRight(condAnd, cond2))
	//t.Run("SetLeft()", testSetLeft(cond, cond2, true))
	//t.Run("SetRight()", testSetRight(cond, cond2, true))
	t.Run("ToString()", testToString(condAnd, "(testCol = 5 AND testCol < 7)"))

	tableD := sqtables.CreateTableDef("testand", colInt)
	row, err := sqtables.CreateRow(profile, 1, tableD, []string{"testCol"}, []sqtypes.Value{sqtypes.NewSQInt(5)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	row2, err := sqtables.CreateRow(profile, 2, tableD, []string{"testCol"}, []sqtypes.Value{sqtypes.NewSQInt(9)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	var tests = []struct {
		Name   string
		Cond   sqtables.Condition
		r      *sqtables.RowDef
		ErrTxt string
		Expect bool
	}{
		{"Evaluate() testCol = 5 AND testCol < 7: 5", condAnd, row, "", true},
		{"Evaluate() testCol = 5 AND testCol < 7: 9", condAnd, row2, "", false},
	}

	for _, tRow := range tests {

		t.Run(tRow.Name, testEvaluate(profile, tRow.Cond, tRow.r, tRow.ErrTxt, tRow.Expect))
	}

}

func TestLogicCondOR(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	colInt := sqtables.CreateColDef("testCol", tokens.TypeInt, false)
	cond := sqtables.NewCVCond(colInt, "=", sqtypes.NewSQInt(5))
	cond2 := sqtables.NewCVCond(colInt, "<", sqtypes.NewSQInt(7))
	//	cond3 := sqtables.NewCVCond(colInt, ">", sqtypes.NewSQInt(7))

	condOR := sqtables.NewORCondition(cond, cond2)
	t.Run("GetLeft()", testGetLeft(condOR, cond))
	t.Run("GetRight()", testGetRight(condOR, cond2))
	//t.Run("SetLeft()", testSetLeft(cond, cond2, true))
	//t.Run("SetRight()", testSetRight(cond, cond2, true))
	t.Run("ToString()", testToString(condOR, "(testCol = 5 OR testCol < 7)"))

	tableD := sqtables.CreateTableDef("testand", colInt)
	row, err := sqtables.CreateRow(profile, 1, tableD, []string{"testCol"}, []sqtypes.Value{sqtypes.NewSQInt(5)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	row2, err := sqtables.CreateRow(profile, 2, tableD, []string{"testCol"}, []sqtypes.Value{sqtypes.NewSQInt(9)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	var tests = []struct {
		Name   string
		Cond   sqtables.Condition
		r      *sqtables.RowDef
		ErrTxt string
		Expect bool
	}{
		{"Evaluate() testCol = 5 OR testCol < 7: 5", condOR, row, "", true},
		{"Evaluate() testCol = 5 OR testCol < 7: 9", condOR, row2, "", false},
	}

	for _, tRow := range tests {

		t.Run(tRow.Name, testEvaluate(profile, tRow.Cond, tRow.r, tRow.ErrTxt, tRow.Expect))
	}

}
