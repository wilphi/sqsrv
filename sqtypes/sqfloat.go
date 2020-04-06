package sqtypes

import (
	"strconv"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/tokens"
)

// SQFloat Methods & Functions  =========================================

// ToString - return string representation of type
func (fp SQFloat) ToString() string {
	return strconv.FormatFloat(fp.Val, 'G', -1, 64)
}

// Type - returns the type
func (fp SQFloat) Type() string {
	return tokens.TypeFloat
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
	vfp, ok := v.(SQFloat)
	ret1 := (fp.Val < vfp.Val)
	ret2 := ok && ret1
	return ret2

}

// GreaterThan -
func (fp SQFloat) GreaterThan(v Value) bool {
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
func (fp SQFloat) Operation(op string, v Value) (retVal Value, err error) {

	// if v is null then the result is null
	if v.IsNull() {
		retVal = v
		return
	}

	vfp, ok := v.(SQFloat)
	if !ok {
		err = sqerr.New("Type Mismatch: " + v.ToString() + " is not a Float")
		return
	}
	switch op {
	case "+":
		retVal = NewSQFloat(fp.Val + vfp.Val)
	case "-":
		retVal = NewSQFloat(fp.Val - vfp.Val)
	case "*":
		retVal = NewSQFloat(fp.Val * vfp.Val)
	case "/":
		retVal = NewSQFloat(fp.Val / vfp.Val)
	case "=":
		retVal = NewSQBool(fp.Val == vfp.Val)
	case "!=":
		retVal = NewSQBool(fp.Val != vfp.Val)
	case "<":
		retVal = NewSQBool(fp.Val < vfp.Val)
	case ">":
		retVal = NewSQBool(fp.Val > vfp.Val)
	case "<=":
		retVal = NewSQBool(fp.Val <= vfp.Val)
	case ">=":
		retVal = NewSQBool(fp.Val >= vfp.Val)
	default:
		err = sqerr.NewSyntax("Invalid Float Operator " + op)
		return
	}
	return

}

// Convert returns the value converted to the given type
func (fp SQFloat) Convert(newtype string) (retVal Value, err error) {
	switch newtype {
	case tokens.TypeInt:
		retVal = NewSQInt(int(fp.Val))
	case tokens.TypeBool:
		retVal = NewSQBool(fp.Val > 0)
	case tokens.TypeFloat:
		retVal = fp
	case tokens.TypeString:
		retVal = NewSQString(strconv.FormatFloat(fp.Val, 'G', -1, 64))
	default:
		err = sqerr.Newf("A value of type %s can not be converted to type %s", fp.Type(), newtype)
	}
	return
}

// NewSQFloat - creates a new SQInt value
func NewSQFloat(fp float64) Value {
	return SQFloat{fp}
}
