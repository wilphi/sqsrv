package sqtypes

import (
	"strconv"
	"strings"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/tokens"
)

// SQString - String type for SQ
type SQString struct {
	Val string
}

// SQString Methods & Functions  =========================================

// ToString - return string representation of type
func (s SQString) ToString() string {
	return s.Val
}

// Type - returns the type
func (s SQString) Type() tokens.TokenID {
	return tokens.String
}

// Len -
func (s SQString) Len() int {
	return -SQStringWidth
}

// Equal - true if values are the same. type mismatch will return false
func (s SQString) Equal(v Value) bool {
	vint, ok := v.(SQString)
	return ok && (s.Val == vint.Val)
}

// LessThan -
func (s SQString) LessThan(v Value) bool {
	vint, ok := v.(SQString)
	return ok && (s.Val < vint.Val)
}

//GreaterThan -
func (s SQString) GreaterThan(v Value) bool {
	vint, ok := v.(SQString)
	return ok && (s.Val > vint.Val)
}

// IsNull - Is the value Null or not
func (s SQString) IsNull() bool {
	return false
}

// Write returns a binary representation of the value
func (s SQString) Write(c *sqbin.Codec) {
	c.Writebyte(SQStringType)
	c.WriteString(s.Val)
}

// Operation transforms two SQString values based on given operator
func (s SQString) Operation(op tokens.TokenID, v Value) (retVal Value, err error) {
	vStr, ok := v.(SQString)
	if !ok {
		if v.IsNull() {
			retVal = v
			return
		}
		err = sqerr.Newf("Type Mismatch: %s is not a String", v.ToString())
		return
	}
	switch op {
	case tokens.Plus:
		retVal = NewSQString(s.Val + vStr.Val)
	case tokens.Equal:
		retVal = NewSQBool(s.Val == vStr.Val)
	case tokens.NotEqual:
		retVal = NewSQBool(s.Val != vStr.Val)
	case tokens.LessThan:
		retVal = NewSQBool(s.Val < vStr.Val)
	case tokens.GreaterThan:
		retVal = NewSQBool(s.Val > vStr.Val)
	case tokens.LessThanEqual:
		retVal = NewSQBool(s.Val <= vStr.Val)
	case tokens.GreaterThanEqual:
		retVal = NewSQBool(s.Val >= vStr.Val)
	default:
		err = sqerr.NewSyntax("Invalid String Operator " + tokens.IDName(op))
		return
	}
	return

}

// Convert returns the value converted to the given type
func (s SQString) Convert(newtype tokens.TokenID) (retVal Value, err error) {
	var i int
	var f float64

	switch newtype {
	case tokens.Int:
		i, err = strconv.Atoi(s.Val)
		if err == nil {
			retVal = NewSQInt(i)
		} else {
			err = sqerr.Newf("Unable to Convert %q to an INT", s.Val)
		}
	case tokens.Bool:
		switch strings.ToUpper(strings.TrimSpace(s.Val)) {
		case "TRUE":
			retVal = NewSQBool(true)
		case "FALSE":
			retVal = NewSQBool(false)
		default:
			err = sqerr.Newf("Unable to convert string to bool")
		}
	case tokens.Float:
		f, err = strconv.ParseFloat(s.Val, 64)
		if err == nil {
			retVal = NewSQFloat(f)
		}
	case tokens.String:
		retVal = s
	default:
		err = sqerr.Newf("A value of type %s can not be converted to type %s", tokens.IDName(s.Type()), tokens.IDName(newtype))
	}
	return
}

// NewSQString - creates a new SQInt value
func NewSQString(s string) Value {
	return SQString{s}
}
