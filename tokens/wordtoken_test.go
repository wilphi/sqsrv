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

type WordTokenData struct {
	TestName string
	ID       tokens.TokenID
	Name     string
	Flag     tokens.TokenFlags
	FlagVal  bool
	String   string
	ExpPanic bool
}

func testWordTokenFunc(d WordTokenData) func(t *testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		//Create token
		tkn := tokens.GetWordToken(d.ID)

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

		//Check String
		if d.String != tkn.String() {
			t.Errorf("Expected String: %s does not match Actual String: %s", d.String, tkn.String())
			return
		}

	}
}

func TestWordToken(t *testing.T) {
	data := []WordTokenData{
		{
			TestName: "Non Word Ident token",
			ID:       tokens.Ident,
			Name:     "[IDENT=test]",
			Flag:     tokens.IsWord,
			FlagVal:  false,
			String:   "[IDENT=2ndTest]",
			ExpPanic: true,
		},
		{
			TestName: "Non Ident token",
			ID:       tokens.Create,
			Name:     "CREATE",
			Flag:     tokens.IsWord,
			FlagVal:  true,
			String:   "CREATE",
			ExpPanic: false,
		},
		{
			TestName: "Invalid token ID",
			ID:       255,
			Name:     "[Create=test]",
			Flag:     tokens.IsWord,
			FlagVal:  false,
			String:   "[IDENT=2ndTest]",
			ExpPanic: true,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testWordTokenFunc(row))

	}

}
