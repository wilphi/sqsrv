package tokens_test

import (
	"testing"

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
		if tl.Test(tokens.Create) != "" {
			t.Error("Unexpected return when Testing an empty list")
		}
	})

	t.Run("Add a Token", func(t *testing.T) {
		tl.Add(tokens.AllWordTokens[tokens.Create])
		if tl.Len() != 1 {
			t.Errorf("Token not added, len=%d", tl.Len())
		}
	})

	t.Run("Peek a Token", func(t *testing.T) {
		if tl.Peek().GetName() != tokens.Create {
			t.Error("Unexpected token when Peeking")
		}
	})
	t.Run("Peekx(0) a Token", func(t *testing.T) {
		if tl.Peekx(0).GetName() != tokens.Create {
			t.Error("Unexpected token when Peekx(0)")
		}
	})
	t.Run("Test for Create token", func(t *testing.T) {
		if tl.Test(tokens.Create) != tokens.Create {
			t.Error("Unexpected return when Testing an empty list")
		}
	})
	t.Run("Test for Wrong token", func(t *testing.T) {
		if tl.Test(tokens.Null) != "" {
			t.Error("Unexpected return when Testing wrong token")
		}
	})
	t.Run("Test for mulitple tokens", func(t *testing.T) {
		if tl.Test(tokens.Create, tokens.Not, tokens.Ident, tokens.Null) != tokens.Create {
			t.Error("Unexpected return when Testing mulitple tokens")
		}
	})

}
