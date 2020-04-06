package sqtypes

import (
	"strconv"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/tokens"
)

// SQBool Methods & Functions  =========================================

// ToString - return string representation of type
func (b SQBool) ToString() string {
	return strconv.FormatBool(b.Val)
}

// Type - returns the type
func (b SQBool) Type() string {
	return tokens.TypeBool
}

// Len -
func (b SQBool) Len() int {
	return SQBoolWidth
}

// Equal - true if values are the same. type mismatch will return false
func (b SQBool) Equal(v Value) bool {
	vint, ok := v.(SQBool)
	return ok && (b.Val == vint.Val)
}

// LessThan -
func (b SQBool) LessThan(v Value) bool {

	return false
}

//GreaterThan -
func (b SQBool) GreaterThan(v Value) bool {

	return false
}

// IsNull - Is the value Null or not
func (b SQBool) IsNull() bool {
	return false
}

// Write returns a binary representation of the value
func (b SQBool) Write(c *sqbin.Codec) {
	if b.Val {
		c.Writebyte(SQBoolTrueType)
	} else {
		c.Writebyte(SQBoolFalseType)
	}
}

// Operation transforms two SQBool values based on given operator
func (b SQBool) Operation(op string, v Value) (retVal Value, err error) {
	vBool, ok := v.(SQBool)
	if !ok {
		if v.IsNull() {
			retVal = v
			return
		}
		err = sqerr.Newf("Type Mismatch: %s is not a Bool", v.ToString())
		return
	}
	switch op {
	case tokens.And:
		retVal = NewSQBool(b.Val && vBool.Val)
	case tokens.Or:
		retVal = NewSQBool(b.Val || vBool.Val)
	case "=":
		retVal = NewSQBool(b.Val == vBool.Val)
	case "!=":
		retVal = NewSQBool(b.Val != vBool.Val)
	default:
		err = sqerr.NewSyntax("Invalid Bool Operator " + op)
		return
	}
	return
}

// Convert returns the value converted to the given type
func (b SQBool) Convert(newtype string) (retVal Value, err error) {
	switch newtype {
	case tokens.TypeInt:
		if b.Val {
			retVal = NewSQInt(1)
		} else {
			retVal = NewSQInt(0)
		}
	case tokens.TypeBool:
		retVal = b
	case tokens.TypeFloat:
		if b.Val {
			retVal = NewSQFloat(1)
		} else {
			retVal = NewSQFloat(0)
		}
	case tokens.TypeString:
		if b.Val {
			retVal = NewSQString("true")
		} else {
			retVal = NewSQString("false")
		}
	default:
		err = sqerr.Newf("A value of type %s can not be converted to type %s", b.Type(), newtype)
	}
	return
}

// NewSQBool - creates a new SQBool value
func NewSQBool(b bool) Value {
	return SQBool{b}
}
