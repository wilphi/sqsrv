package tokens_test

import (
	"fmt"
	"testing"

	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("tokens_test.log")
}

type ValueTokenData struct {
	TestName  string
	ID        tokens.TokenID
	Name      string
	Value     string
	Flag      tokens.TokenFlags
	FlagVal   bool
	SecondVal string
	String    string
	ExpPanic  bool
}

func testValueTokenFunc(d ValueTokenData) func(t *testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		//Create token
		tkn := tokens.NewValueToken(d.ID, d.Value)

		// Check ID
		if d.ID != tkn.ID() {
			t.Errorf("Expected ID: %s does not match Actual ID: %s", tokens.IDName(d.ID), tokens.IDName(tkn.ID()))
			return
		}

		//Check Name
		if d.Name != tkn.Name() {
			t.Errorf("Expected Name: %s does not match Actual Name: %s", d.Name, tkn.Name())
			return
		}

		// Check Flag
		if tkn.TestFlags(d.Flag) != d.FlagVal {
			t.Errorf("TestFlag value: %t did not match expected: %t", tkn.TestFlags(d.Flag), d.FlagVal)
		}

		// Check Value
		vtkn := tkn.(*tokens.ValueToken)
		if d.Value != vtkn.Value() {
			t.Errorf("Expected Value: %s does not match Actual Value: %s", d.Value, vtkn.Value())
			return
		}

		// Check Set & Get Value
		vtkn.SetValue(d.SecondVal)
		if d.SecondVal != vtkn.Value() {
			t.Errorf("Expected Value: %s does not match Actual Value: %s", d.SecondVal, vtkn.Value())
			return
		}
		//Check String
		if d.String != tkn.String() {
			t.Errorf("Expected String: %s does not match Actual String: %s", d.String, tkn.String())
			return
		}

	}
}

func TestValueToken(t *testing.T) {
	data := []ValueTokenData{
		{
			TestName:  "Ident token",
			ID:        tokens.Ident,
			Name:      "[IDENT=test]",
			Value:     "test",
			Flag:      tokens.IsWord,
			FlagVal:   false,
			SecondVal: "2ndTest",
			String:    "[IDENT=2ndTest]",
			ExpPanic:  false,
		},
		{
			TestName:  "Non Ident token",
			ID:        tokens.Create,
			Name:      "[Create=test]",
			Value:     "test",
			Flag:      tokens.IsWord,
			FlagVal:   false,
			SecondVal: "2ndTest",
			String:    "[IDENT=2ndTest]",
			ExpPanic:  true,
		},
		{
			TestName:  "Invalid token ID",
			ID:        255,
			Name:      "[Create=test]",
			Value:     "test",
			Flag:      tokens.IsWord,
			FlagVal:   false,
			SecondVal: "2ndTest",
			String:    "[IDENT=2ndTest]",
			ExpPanic:  true,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testValueTokenFunc(row))

	}

}
