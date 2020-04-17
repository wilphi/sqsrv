package sqtypes

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"

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
	SQNullType      = 0
	SQIntType       = 1
	SQStringType    = 2
	SQBoolTrueType  = 3
	SQBoolFalseType = 4
	SQFloatType     = 5
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
}

// Negatable is an interface for Values that can be negated
type Negatable interface {
	Negate() Value
}

//Raw is a type that can be converted into sq Values
type Raw interface{}

// RawVals raw values that can be converted to sq Values
// 	used for testing
type RawVals [][]Raw

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

//RawValue given any type convert it into a SQ Value
// Currently only works for int, string, bool
//  nil values get converted to SQNull
func RawValue(raw Raw) Value {
	var retVal Value

	if raw == nil {
		return NewSQNull()
	}
	switch reflect.TypeOf(raw).Kind() {
	case reflect.Int:
		retVal = NewSQInt(raw.(int))
	case reflect.String:
		retVal = NewSQString(raw.(string))
	case reflect.Bool:
		retVal = NewSQBool(raw.(bool))
	case reflect.Float32:
		retVal = NewSQFloat(float64(raw.(float32)))
	case reflect.Float64:
		retVal = NewSQFloat(raw.(float64))
	default:
		panic(fmt.Sprintf("%T is not a valid Raw SQ type", raw))
	}
	return retVal
}

// CreateValuesFromRaw converts a 2D array of raw to a 2D array of sqtypes.Value
func CreateValuesFromRaw(raw RawVals) [][]Value {
	nRows := len(raw)
	retVals := make([][]Value, nRows)

	for i, row := range raw {
		retVals[i] = CreateValueArrayFromRaw(row)
	}
	return retVals

}

// CreateValueArrayFromRaw converts an array of raw to an array of sqtypes.Value
func CreateValueArrayFromRaw(rawArray []Raw) []Value {
	retVals := make([]Value, len(rawArray))
	for j, item := range rawArray {
		retVals[j] = RawValue(item)
	}
	return retVals
}

// Compare2DValue - returns "" is arrays match otherwise a string describing where the arrays do not match.
func Compare2DValue(a, b [][]Value, aName, bName string, doSort bool) string {
	if len(a) != len(b) {
		return fmt.Sprintf("The number of rows does not match! %s(%d) %s(%d)", aName, len(a), bName, len(b))
	}

	for i := range a {
		if len(a[i]) != len(b[i]) {
			return fmt.Sprintf("The number of cols does not match! %s[%d]-len=%d %s[%d]-len=%d", aName, i, len(a[i]), bName, i, len(b[i]))
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
				return fmt.Sprintf("Type Mismatch: %s[%d][%d] = %s Does not match %s[%d][%d] = %s", aName, i, j, tokens.IDName(a[i][j].Type()), bName, i, j, tokens.IDName(b[i][j].Type()))
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
