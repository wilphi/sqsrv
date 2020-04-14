package tokens

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

// ValueToken is for storing identifiers
type ValueToken struct {
	tokenID    TokenID
	tokenValue string
}

// Value Token Constants
const (
	Ident = 1<<7 + iota
	Quote
	Num
	Err
	Unk
)

// ValueTokenNames contains the list of value token names
var valueTokenNames = map[TokenID]string{Ident: "IDENT", Quote: "QUOTE", Num: "NUM", Err: "ERR", Unk: "UNK"}

// ID returns the Id of the token
func (tkn *ValueToken) ID() TokenID {
	return tkn.tokenID
}

// Name returns the text name of the token
func (tkn *ValueToken) Name() string {
	return fmt.Sprintf("[%s=%s]", valueTokenNames[tkn.tokenID], tkn.tokenValue)
}

// String returns a string representation of the token
//   This may or may not be the same as the token Name
func (tkn *ValueToken) String() string {
	return tkn.Name()
}

// TestFlags -
func (tkn *ValueToken) TestFlags(mask TokenFlags) bool {
	return false
}

// SetValue set the value of a value token
func (tkn *ValueToken) SetValue(value string) {
	tkn.tokenValue = value
}

// Value returns the value of a value token
func (tkn *ValueToken) Value() string {
	return tkn.tokenValue
}

// NewValueToken creates a token with a value
func NewValueToken(ID TokenID, value string) Token {
	_, ok := valueTokenNames[ID]
	if !ok {
		log.Panicf("ID: %s is not a valid ValueToken id", IDName(ID))
	}
	return &ValueToken{tokenID: ID, tokenValue: value}
}
