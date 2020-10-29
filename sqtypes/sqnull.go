package sqtypes

import (
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/tokens"
)

// SQNull - Null value for SQ
type SQNull struct {
}

var originalNull = &SQNull{}

//SQNull Methods & Functions ============================================

// String - return string representation of type
func (n SQNull) String() string {
	return tokens.IDName(tokens.Null)
}

// Type - returns the type
func (n SQNull) Type() tokens.TokenID {
	return tokens.Null
}

// Len -
func (n SQNull) Len() int {
	return 7
}

// Equal - Null values are never equal
func (n SQNull) Equal(v Value) bool {
	return false
}

// LessThan -
func (n SQNull) LessThan(v Value) bool {

	return false
}

//GreaterThan -
func (n SQNull) GreaterThan(v Value) bool {
	return false
}

// IsNull - Is the value Null or not
func (n SQNull) IsNull() bool {
	return true
}

// Write returns a binary representation of the value
func (n SQNull) Write(c *sqbin.Codec) {
	c.Writebyte(SQNullType)
}

// Operation is always NULL for Null values
func (n SQNull) Operation(op tokens.TokenID, v Value) (Value, error) {
	return originalNull, nil
}

// Convert returns the value converted to the given type
func (n SQNull) Convert(newtype tokens.TokenID) (retVal Value, err error) {
	return originalNull, nil
}

// NewSQNull - creates a new SQNull value
func NewSQNull() Value {
	return originalNull
}

// Negate returns minus the current value
func (n SQNull) Negate() Value {
	return NewSQNull()
}

// Clone creates a deep copy of the Value
func (n SQNull) Clone() Value {
	return NewSQNull()
}
