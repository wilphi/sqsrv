package sqtypes

import (
	"strconv"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/tokens"
)

// SQInt - Integer type for SQ
type SQInt struct {
	Val int
}

// SQInt Methods & Functions  =========================================

// String - return string representation of type
func (i SQInt) String() string {
	return strconv.Itoa(i.Val)
}

// Type - returns the type
func (i SQInt) Type() tokens.TokenID {
	return tokens.Int
}

// Len -
func (i SQInt) Len() int {
	return SQIntWidth
}

// Equal - true if values are the same. type mismatch will return false
func (i SQInt) Equal(v Value) bool {
	vint, ok := v.(SQInt)
	comp := (i.Val == vint.Val)
	return ok && comp
}

// LessThan -
func (i SQInt) LessThan(v Value) bool {
	if v.IsNull() {
		return true
	}
	vint, ok := v.(SQInt)
	ret := ok && (i.Val < vint.Val)
	return ret

}

// GreaterThan -
func (i SQInt) GreaterThan(v Value) bool {
	if v.IsNull() {
		return false
	}
	vint, ok := v.(SQInt)
	return ok && (i.Val > vint.Val)

}

// IsNull - Is the value Null or not
func (i SQInt) IsNull() bool {
	return false
}

// Write returns a binary representation of the value
func (i SQInt) Write(c *sqbin.Codec) {
	c.Writebyte(SQIntType)
	c.WriteInt(i.Val)

}

// Operation transforms two SQInt values based on given operator
func (i SQInt) Operation(op tokens.TokenID, v Value) (retVal Value, err error) {

	vint, ok := v.(SQInt)
	if !ok {
		if v.IsNull() {
			retVal = v
			return
		}
		err = sqerr.New("Type Mismatch: " + v.String() + " is not an Int")
		return
	}
	switch op {
	case tokens.Plus:
		retVal = NewSQInt(i.Val + vint.Val)
	case tokens.Minus:
		retVal = NewSQInt(i.Val - vint.Val)
	case tokens.Asterix:
		retVal = NewSQInt(i.Val * vint.Val)
	case tokens.Divide:
		retVal = NewSQInt(i.Val / vint.Val)
	case tokens.Modulus:
		retVal = NewSQInt(i.Val % vint.Val)
	case tokens.Equal:
		retVal = NewSQBool(i.Val == vint.Val)
	case tokens.NotEqual:
		retVal = NewSQBool(i.Val != vint.Val)
	case tokens.LessThan:
		retVal = NewSQBool(i.Val < vint.Val)
	case tokens.GreaterThan:
		retVal = NewSQBool(i.Val > vint.Val)
	case tokens.LessThanEqual:
		retVal = NewSQBool(i.Val <= vint.Val)
	case tokens.GreaterThanEqual:
		retVal = NewSQBool(i.Val >= vint.Val)
	default:
		err = sqerr.NewSyntax("Invalid Int Operator " + tokens.IDName(op))
		return
	}
	return

}

// Convert returns the value converted to the given type
func (i SQInt) Convert(newtype tokens.TokenID) (retVal Value, err error) {
	switch newtype {
	case tokens.Int:
		retVal = i
	case tokens.Bool:
		retVal = NewSQBool(i.Val > 0)
	case tokens.Float:
		retVal = NewSQFloat(float64(i.Val))
	case tokens.String:
		retVal = NewSQString(strconv.Itoa(i.Val))
	default:
		err = sqerr.Newf("A value of type %s can not be converted to type %s", tokens.IDName(i.Type()), tokens.IDName(newtype))
	}
	return
}

// NewSQInt - creates a new SQInt value
func NewSQInt(i int) Value {
	return SQInt{i}
}

// Negate returns minus the current value
func (i SQInt) Negate() Value {
	return NewSQInt(-i.Val)
}
