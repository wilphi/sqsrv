package tokens_test

import (
	"fmt"
	"testing"

	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func TestTokenList(t *testing.T) {
	tl := tokens.NewTokenList()

	t.Run("Len=0", func(t *testing.T) {
		if tl.Len() != 0 {
			t.Error("Length of tokenlist should = 0")
		}
	})
	t.Run("Remove from empty tokenlist", func(t *testing.T) {
		tl.Remove()
		if tl.Len() != 0 {
			t.Error("Remove: Length of tokenlist should = 0")
		}
	})
	t.Run("Peek when no tokens", func(t *testing.T) {
		if tl.Peek() != nil {
			t.Error("Unexpected token when Peeking an empty list")
		}
	})
	t.Run("Peekx(0) when no tokens", func(t *testing.T) {
		if tl.Peekx(0) != nil {
			t.Error("Unexpected token when Peekx(0) an empty list")
		}
	})
	t.Run("Peekx(1) when no tokens", func(t *testing.T) {
		if tl.Peekx(1) != nil {
			t.Error("Unexpected token when Peekx(1) an empty list")
		}
	})

	t.Run("Test when no tokens", func(t *testing.T) {
		if tl.TestTkn(tokens.Create) != nil {
			t.Error("Unexpected return when Testing an empty list")
		}
	})

	t.Run("Add a Token", func(t *testing.T) {
		tl.Add(tokens.GetWordToken(tokens.Create))
		if tl.Len() != 1 {
			t.Errorf("Token not added, len=%d", tl.Len())
		}
	})

	t.Run("Peek a Token", func(t *testing.T) {
		if tl.Peek().ID() != tokens.Create {
			t.Error("Unexpected token when Peeking")
		}
	})
	t.Run("Peekx(0) a Token", func(t *testing.T) {
		if tl.Peekx(0).ID() != tokens.Create {
			t.Error("Unexpected token when Peekx(0)")
		}
	})
	t.Run("Test for Create token", func(t *testing.T) {
		if tl.TestTkn(tokens.Create).ID() != tokens.Create {
			t.Error("Unexpected return when Testing an empty list")
		}
	})
	t.Run("Test for Wrong token", func(t *testing.T) {
		if tl.TestTkn(tokens.Null) != nil {
			t.Error("Unexpected return when Testing wrong token")
		}
	})
	t.Run("Test for mulitple tokens", func(t *testing.T) {
		if tl.TestTkn(tokens.Create, tokens.Not, tokens.Ident, tokens.Null).ID() != tokens.Create {
			t.Error("Unexpected return when Testing mulitple tokens")
		}
	})

}

func TestTList(t *testing.T) {

	data := []TListData{
		{
			TestName: "Add to Empty List",
			TestStr:  "",
			AddTkns:  []tokens.Token{tokens.GetWordToken(tokens.Select)},
			ExpLen:   1,
			ExpList:  "SELECT",
			IsA:      tokens.Select,
			IsAret:   true,
			IsWord:   true,
		},
		{
			TestName: "Add to List",
			TestStr:  "SELECT * FROM ",
			AddTkns:  []tokens.Token{tokens.NewValueToken(tokens.Ident, "tableA")},
			ExpLen:   4,
			ExpList:  "SELECT * FROM [IDENT=tableA]",
			IsA:      tokens.Ident,
			IsAret:   false,
			IsWord:   true,
		},
		{
			TestName: "Add multiple to List",
			TestStr:  "SELECT * FROM ",
			AddTkns:  []tokens.Token{tokens.NewValueToken(tokens.Ident, "tableA"), tokens.GetWordToken(tokens.Where), tokens.NewValueToken(tokens.Ident, "col1")},
			ExpLen:   6,
			ExpList:  "SELECT * FROM [IDENT=tableA] WHERE [IDENT=col1]",
			IsWord:   true,
		},
		{
			TestName: "Remove from Empty List",
			TestStr:  "",
			AddTkns:  nil,
			Remove:   1,
			ExpLen:   0,
			ExpList:  "",
			IsA:      tokens.Ident,
			IsAret:   false,
			IsWord:   false,
		},
		{
			TestName: "Remove from List",
			TestStr:  "SELECT * FROM ",
			AddTkns:  nil,
			Remove:   1,
			ExpLen:   2,
			ExpList:  "* FROM",
			IsWord:   false,
		},
		{
			TestName: "Empty out List",
			TestStr:  "SELECT * FROM ",
			AddTkns:  nil,
			Remove:   4,
			ExpLen:   0,
			ExpList:  "",
			IsWord:   false,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testTListFunc(row))

	}

}

type TListData struct {
	TestName string
	TestStr  string
	AddTkns  []tokens.Token
	Remove   int
	IsA      tokens.TokenID
	IsAret   bool
	IsWord   bool
	ExpLen   int
	ExpList  string
}

func testTListFunc(d TListData) func(t *testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tkns := tokens.Tokenize(d.TestStr)
		if d.AddTkns != nil {
			for _, tkn := range d.AddTkns {
				tkns.Add(tkn)
			}
		}
		for i := 0; i < d.Remove; i++ {
			tkns.Remove()
		}

		if tkns.IsReservedWord() != d.IsWord {
			t.Errorf("IsReservedWord was %t when is should have been %t", !d.IsWord, d.IsWord)
			return
		}
		if tkns.IsA(d.IsA) != d.IsAret {
			t.Errorf("IsA(%s) returned %t when it should not have", tokens.IDName(d.IsA), !d.IsAret)
			return
		}
		// Check IsEmpty
		if tkns.IsEmpty() != (d.ExpLen <= 0) {
			t.Errorf("IsEmpty = %t does not match expected %t", tkns.IsEmpty(), (d.ExpLen <= 0))
			return
		}
		if tkns.Len() != d.ExpLen {
			t.Errorf("Actual Len %d does not match Expected %d", tkns.Len(), d.ExpLen)
			return
		}
		if tkns.String() != d.ExpList {
			t.Errorf("Token list %q does not match expected list %q", tkns.String(), d.ExpList)
			return
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func TestIsARemove(t *testing.T) {

	data := []IsARemoveData{
		{
			TestName: "Empty List",
			TestStr:  "",
			ExpLen:   0,
			ExpList:  "",
			IsA:      tokens.Select,
			IsAret:   false,
		},
		{
			TestName: "Select Only",
			TestStr:  "SELECT ",
			ExpLen:   0,
			ExpList:  "",
			IsA:      tokens.Select,
			IsAret:   true,
		},
		{
			TestName: "Select at beginning of statement",
			TestStr:  "SELECT * from test",
			ExpLen:   3,
			ExpList:  "* FROM [IDENT=test]",
			IsA:      tokens.Select,
			IsAret:   true,
		},
		{
			TestName: "no Select",
			TestStr:  "INSERT INTO test",
			ExpLen:   3,
			ExpList:  "INSERT INTO [IDENT=test]",
			IsA:      tokens.Select,
			IsAret:   false,
		},
		{
			TestName: "INSERT ",
			TestStr:  "INSERT INTO test",
			ExpLen:   2,
			ExpList:  "INTO [IDENT=test]",
			IsA:      tokens.Insert,
			IsAret:   true,
		}}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testIsARemoveFunc(row))

	}

}

type IsARemoveData struct {
	TestName string
	TestStr  string
	IsA      tokens.TokenID
	IsAret   bool
	ExpLen   int
	ExpList  string
}

func testIsARemoveFunc(d IsARemoveData) func(t *testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tkns := tokens.Tokenize(d.TestStr)

		if tkns.IsARemove(d.IsA) != d.IsAret {
			t.Errorf("IsA(%s) returned %t when it should not have", tokens.IDName(d.IsA), !d.IsAret)
			return
		}

		if tkns.Len() != d.ExpLen {
			t.Errorf("Actual Len %d does not match Expected %d", tkns.Len(), d.ExpLen)
			return
		}
		if tkns.String() != d.ExpList {
			t.Errorf("Token list %q does not match expected list %q", tkns.String(), d.ExpList)
			return
		}
	}
}
