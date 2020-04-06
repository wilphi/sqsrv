package sqtypes

import (
	"strconv"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/tokens"
)

// SQInt Methods & Functions  =========================================

// ToString - return string representation of type
func (i SQInt) ToString() string {
	return strconv.Itoa(i.Val)
}

// Type - returns the type
func (i SQInt) Type() string {
	return tokens.TypeInt
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
	vint, ok := v.(SQInt)
	ret1 := (i.Val < vint.Val)
	ret2 := ok && ret1
	return ret2

}

// GreaterThan -
func (i SQInt) GreaterThan(v Value) bool {
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
func (i SQInt) Operation(op string, v Value) (retVal Value, err error) {

	vint, ok := v.(SQInt)
	if !ok {
		if v.IsNull() {
			retVal = v
			return
		}
		err = sqerr.New("Type Mismatch: " + v.ToString() + " is not an Int")
		return
	}
	switch op {
	case "+":
		retVal = NewSQInt(i.Val + vint.Val)
	case "-":
		retVal = NewSQInt(i.Val - vint.Val)
	case "*":
		retVal = NewSQInt(i.Val * vint.Val)
	case "/":
		retVal = NewSQInt(i.Val / vint.Val)
	case "%":
		retVal = NewSQInt(i.Val % vint.Val)
	case "=":
		retVal = NewSQBool(i.Val == vint.Val)
	case "!=":
		retVal = NewSQBool(i.Val != vint.Val)
	case "<":
		retVal = NewSQBool(i.Val < vint.Val)
	case ">":
		retVal = NewSQBool(i.Val > vint.Val)
	case "<=":
		retVal = NewSQBool(i.Val <= vint.Val)
	case ">=":
		retVal = NewSQBool(i.Val >= vint.Val)
	default:
		err = sqerr.NewSyntax("Invalid Int Operator " + op)
		return
	}
	return

}

// Convert returns the value converted to the given type
func (i SQInt) Convert(newtype string) (retVal Value, err error) {
	switch newtype {
	case tokens.TypeInt:
		retVal = i
	case tokens.TypeBool:
		retVal = NewSQBool(i.Val > 0)
	case tokens.TypeFloat:
		retVal = NewSQFloat(float64(i.Val))
	case tokens.TypeString:
		retVal = NewSQString(strconv.Itoa(i.Val))
	default:
		err = sqerr.Newf("A value of type %s can not be converted to type %s", i.Type(), newtype)
	}
	return
}

// NewSQInt - creates a new SQInt value
func NewSQInt(i int) Value {
	return SQInt{i}
}
