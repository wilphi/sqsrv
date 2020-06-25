package tokens

import (
	"fmt"
	"strings"
)

// TokenList - Structure to contain a list of Tokens
type TokenList struct {
	tkns []Token
}

// Add - Add a token to the list
func (tl *TokenList) Add(tkn Token) {
	tl.tkns = append(tl.tkns, tkn)
}

// Remove - Remove a token from the list
func (tl *TokenList) Remove() {
	if len(tl.tkns) > 0 {
		tl.tkns[0] = nil
		tl.tkns = tl.tkns[1:]
	}
}

// Peek - returns the token at head of list
func (tl *TokenList) Peek() Token {
	if len(tl.tkns) == 0 {
		return nil
	}
	return tl.tkns[0]
}

// Peekx - returns the token at list[x]
func (tl *TokenList) Peekx(x int) Token {
	if len(tl.tkns) <= x || x < 0 {
		return nil
	}
	return tl.tkns[x]
}

// Len - Number of tokens in list
func (tl *TokenList) Len() int {
	return len(tl.tkns)
}

// IsEmpty tests if the token list is empty or not
func (tl *TokenList) IsEmpty() bool {
	return tl.Len() <= 0
}

// String - returns a string representation of list
func (tl *TokenList) String() string {
	var b strings.Builder

	for _, tkn := range tl.tkns {
		fmt.Fprint(&b, tkn.String(), " ")
		//		output = output + tkn.String() + " "
	}
	output := b.String()
	// Remove trailing space
	if len(output) > 0 {
		output = output[:len(output)-1]
	}

	return output
}

// TestTkn - Test a token to see if it matches one of the tknNames.
//  Returns the token if matched otherwise nil
//  If there are no more tokens in list nil is returned as well
func (tl *TokenList) TestTkn(tkns ...TokenID) Token {
	if len(tl.tkns) > 0 {
		for _, tkn := range tkns {
			if tl.tkns[0].ID() == tkn {
				return tl.tkns[0]
			}
		}
	}
	return nil
}

// IsA - tests a tokens to see if is a match
func (tl *TokenList) IsA(tkn TokenID) bool {
	if len(tl.tkns) > 0 {
		if tl.tkns[0].ID() == tkn {
			return true
		}

	}
	return false
}

// IsARemove - tests a tokens to see if is a match. If so then removes it.
func (tl *TokenList) IsARemove(tkn TokenID) bool {
	if len(tl.tkns) > 0 {
		if tl.tkns[0].ID() == tkn {
			tl.Remove()
			return true
		}

	}
	return false
}

// IsReservedWord - checks to see if the first token in list is a reserved word token
func (tl *TokenList) IsReservedWord() bool {
	if len(tl.tkns) > 0 {
		return tl.tkns[0].TestFlags(IsWord)
	}
	return false
}

// NewTokenList - Create a new token list
func NewTokenList() *TokenList {
	tl := TokenList{}
	tl.tkns = make([]Token, 0, 100)
	return &tl
}

// CreateList - Creates a new token list from an array of tokens
func CreateList(tkns []Token) *TokenList {
	tl := TokenList{}
	tl.tkns = tkns
	return &tl
}
