package sqtypes

import (
	"strconv"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/tokens"
)

// SQFloat - Floating point type for SQ
type SQFloat struct {
	Val float64
}

// SQFloat Methods & Functions  =========================================

// String - return string representation of type
func (fp SQFloat) String() string {
	return strconv.FormatFloat(fp.Val, 'G', -1, 64)
}

// Type - returns the type
func (fp SQFloat) Type() tokens.TokenID {
	return tokens.Float
}

// Len -
func (fp SQFloat) Len() int {
	return SQFloatWidth
}

// Equal - true if values are the same. type mismatch will return false
func (fp SQFloat) Equal(v Value) bool {
	vfp, ok := v.(SQFloat)
	return ok && (fp.Val == vfp.Val)
}

// LessThan -
func (fp SQFloat) LessThan(v Value) bool {
	if v.IsNull() {
		return true
	}
	vfp, ok := v.(SQFloat)
	ret1 := (fp.Val < vfp.Val)
	ret2 := ok && ret1
	return ret2

}

// GreaterThan -
func (fp SQFloat) GreaterThan(v Value) bool {
	if v.IsNull() {
		return false
	}
	vfp, ok := v.(SQFloat)
	return ok && (fp.Val > vfp.Val)

}

// IsNull - Is the value Null or not
func (fp SQFloat) IsNull() bool {
	return false
}

// Write returns a binary representation of the value
func (fp SQFloat) Write(c *sqbin.Codec) {
	c.Writebyte(SQFloatType)
	c.WriteFloat(fp.Val)
}

// Operation transforms two SQFloat values based on given operator
func (fp SQFloat) Operation(op tokens.TokenID, v Value) (retVal Value, err error) {

	// if v is null then the result is null
	if v.IsNull() {
		retVal = v
		return
	}

	vfp, ok := v.(SQFloat)
	if !ok {
		err = sqerr.New("Type Mismatch: " + v.String() + " is not a Float")
		return
	}
	switch op {
	case tokens.Plus:
		retVal = NewSQFloat(fp.Val + vfp.Val)
	case tokens.Minus:
		retVal = NewSQFloat(fp.Val - vfp.Val)
	case tokens.Asterix:
		retVal = NewSQFloat(fp.Val * vfp.Val)
	case tokens.Divide:
		retVal = NewSQFloat(fp.Val / vfp.Val)
	case tokens.Equal:
		retVal = NewSQBool(fp.Val == vfp.Val)
	case tokens.NotEqual:
		retVal = NewSQBool(fp.Val != vfp.Val)
	case tokens.LessThan:
		retVal = NewSQBool(fp.Val < vfp.Val)
	case tokens.GreaterThan:
		retVal = NewSQBool(fp.Val > vfp.Val)
	case tokens.LessThanEqual:
		retVal = NewSQBool(fp.Val <= vfp.Val)
	case tokens.GreaterThanEqual:
		retVal = NewSQBool(fp.Val >= vfp.Val)
	default:
		err = sqerr.NewSyntax("Invalid Float Operator " + tokens.IDName(op))
		return
	}
	return

}

// Convert returns the value converted to the given type
func (fp SQFloat) Convert(newtype tokens.TokenID) (retVal Value, err error) {
	switch newtype {
	case tokens.Int:
		retVal = NewSQInt(int(fp.Val))
	case tokens.Bool:
		retVal = NewSQBool(fp.Val > 0)
	case tokens.Float:
		retVal = fp
	case tokens.String:
		retVal = NewSQString(strconv.FormatFloat(fp.Val, 'G', -1, 64))
	default:
		err = sqerr.Newf("A value of type %s can not be converted to type %s", tokens.IDName(fp.Type()), tokens.IDName(newtype))
	}
	return
}

// NewSQFloat - creates a new SQInt value
func NewSQFloat(fp float64) Value {
	return SQFloat{fp}
}

// Negate returns minus the current value
func (fp SQFloat) Negate() Value {
	return NewSQFloat(-fp.Val)
}

// Clone creates a deep copy of the Value
func (fp SQFloat) Clone() Value {
	return NewSQFloat(fp.Val)
}
