package sqtypes

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/tokens"
)

// standard Widths for value types
const (
	SQIntWidth    = 10
	SQStringWidth = 30
	SQBoolWidth   = 6
	SQFloatWidth  = 24
)

// Value TypeIDs
const (
	SQNullType = iota + 32
	SQIntType
	SQStringType
	SQBoolTrueType
	SQBoolFalseType
	SQFloatType
)

// Value interface - All Values must be Immutable
type Value interface {
	String() string
	Type() tokens.TokenID
	Len() int
	Equal(v Value) bool
	LessThan(v Value) bool
	GreaterThan(v Value) bool
	IsNull() bool
	Write(c *sqbin.Codec)
	Operation(op tokens.TokenID, v Value) (Value, error)
	Convert(newtype tokens.TokenID) (Value, error)
	Clone() Value
}

// ValueArray is a 1-D array of values
type ValueArray []Value

// ValueMatrix is an array of ValueArrays
type ValueMatrix [][]Value

func init() {
	sqbin.RegisterType("SQNull", SQNullType)
	sqbin.RegisterType("SQInt", SQIntType)
	sqbin.RegisterType("SQString", SQStringType)
	sqbin.RegisterType("SQBoolTrue", SQBoolTrueType)
	sqbin.RegisterType("SQBoolFalse", SQBoolFalseType)
	sqbin.RegisterType("SQFloat", SQFloatType)

}

// Negatable is an interface for Values that can be negated
type Negatable interface {
	Negate() Value
}

// ReadValue takes a byte array an decodes the Value from it.
// 	Int returns the number of bytes read
func ReadValue(c *sqbin.Codec) Value {
	var ret Value
	b := c.Readbyte()
	switch b {
	case SQNullType:
		ret = NewSQNull()
	case SQBoolTrueType:
		ret = NewSQBool(true)
	case SQBoolFalseType:
		ret = NewSQBool(false)
	case SQIntType:
		i := c.ReadInt()
		ret = NewSQInt(i)
	case SQStringType:
		str := c.ReadString()
		ret = NewSQString(str)
	case SQFloatType:
		fp := c.ReadFloat()
		ret = NewSQFloat(fp)
	default:
		log.Panicf("Unknown Value TypeID %d", b)
	}
	return ret
}

//====================================================================

// CreateValueFromToken - given a token, convert it into a proper Value
func CreateValueFromToken(tkn tokens.Token) (Value, error) {
	var retVal Value

	switch tkn.ID() {
	case tokens.Num:
		val := tkn.(*tokens.ValueToken).Value()
		// try to convert to int
		i, err := strconv.Atoi(val)
		if err != nil {
			//If not Int try to convert to float64
			fp, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, sqerr.NewSyntaxf("%q is not a number", val)
			}
			retVal = NewSQFloat(fp)
		} else {
			retVal = NewSQInt(i)
		}
	case tokens.Quote:
		val := tkn.(*tokens.ValueToken).Value()
		retVal = NewSQString(val)
	case tokens.RWTrue:
		retVal = NewSQBool(true)
	case tokens.RWFalse:
		retVal = NewSQBool(false)
	case tokens.Null:
		retVal = NewSQNull()
	default:
		return nil, sqerr.NewInternalf("%q is not a valid Value", tkn.String())
	}
	return retVal, nil
}

// Compare2DValue - returns "" is arrays match otherwise a string describing where the arrays do not match.
func Compare2DValue(a, b ValueMatrix, aName, bName string, doSort bool) string {
	return CompareValueMatrix(a, b, aName, bName, doSort)
}

//CompareValueMatrix returns "" is arrays match otherwise a string describing where the arrays do not match.
func CompareValueMatrix(a, b ValueMatrix, aName, bName string, doSort bool) string {
	if len(a) != len(b) {
		return fmt.Sprintf("The number of rows does not match! %s(%d) %s(%d)", aName, len(a), bName, len(b))
	}

	for i := range a {
		if len(a[i]) != len(b[i]) {
			return fmt.Sprintf("The number of cols does not match! %s[%d]-len=%d %s[%d]-len=%d", aName, i, len(a[i]), bName, i, len(b[i])) +
				fmt.Sprintf("\n%s[%d] = %v Does not match %s[%d] = %v", aName, i, a[i], bName, i, b[i])
		}
	}
	if doSort {
		for x := len(a[0]) - 1; x >= 0; x-- {
			sort.SliceStable(a, func(i, j int) bool { return a[i][x].LessThan(a[j][x]) })
			sort.SliceStable(b, func(i, j int) bool { return b[i][x].LessThan(b[j][x]) })

		}
	}
	for i, row := range a {
		for j, val := range row {
			if val.Type() != b[i][j].Type() {
				return fmt.Sprintf("Type Mismatch: %s[%d][%d] = %s Does not match %s[%d][%d] = %s", aName, i, j, strings.ToLower(tokens.IDName(a[i][j].Type())), bName, i, j, strings.ToLower(tokens.IDName(b[i][j].Type())))
			}
			if !val.Equal(b[i][j]) {
				if !(val.IsNull() && b[i][j].IsNull()) {
					return fmt.Sprintf("%s[%d] = %v Does not match %s[%d] = %v", aName, i, a[i], bName, i, b[i])
				}
			}
		}
	}
	return ""
}

// Clone creates a deep copy
func (a ValueArray) Clone() ValueArray {
	ret := make(ValueArray, len(a))

	for i, v := range a {
		ret[i] = v.Clone()
	}
	return ret
}

// Clone creates a deep copy
func (a ValueMatrix) Clone() ValueMatrix {
	ret := make(ValueMatrix, len(a))

	for i, v := range a {
		va := ValueArray(v)
		ret[i] = va.Clone()

	}
	return ret
}
