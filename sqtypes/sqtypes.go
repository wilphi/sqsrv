package sqtypes

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

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

// Value interface
type Value interface {
	ToString() string
	Type() string
	Len() int
	Equal(v Value) bool
	LessThan(v Value) bool
	GreaterThan(v Value) bool
	IsNull() bool
	Write(c *sqbin.Codec)
	Operation(op string, v Value) (Value, error)
	Convert(newtype string) (Value, error)
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

// SQInt - Integer type for SQ
type SQInt struct {
	Val int
}

// SQString - String type for SQ
type SQString struct {
	Val string
}

// SQBool - Bool type for SQ
type SQBool struct {
	Val bool
}

// SQNull - Null value for SQ
type SQNull struct {
}

// SQFloat - Floating point type for SQ
type SQFloat struct {
	Val float64
}

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
	return ok && (i.Val == vint.Val)
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

// SQString Methods & Functions  =========================================

// ToString - return string representation of type
func (s SQString) ToString() string {
	return s.Val
}

// Type - returns the type
func (s SQString) Type() string {
	return tokens.TypeString
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
func (s SQString) Operation(op string, v Value) (retVal Value, err error) {
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
	case "+":
		retVal = NewSQString(s.Val + vStr.Val)
	case "=":
		retVal = NewSQBool(s.Val == vStr.Val)
	case "!=":
		retVal = NewSQBool(s.Val != vStr.Val)
	case "<":
		retVal = NewSQBool(s.Val < vStr.Val)
	case ">":
		retVal = NewSQBool(s.Val > vStr.Val)
	case "<=":
		retVal = NewSQBool(s.Val <= vStr.Val)
	case ">=":
		retVal = NewSQBool(s.Val >= vStr.Val)
	default:
		err = sqerr.NewSyntax("Invalid String Operator " + op)
		return
	}
	return

}

// Convert returns the value converted to the given type
func (s SQString) Convert(newtype string) (retVal Value, err error) {
	var i int
	var f float64

	switch newtype {
	case tokens.TypeInt:
		i, err = strconv.Atoi(s.Val)
		if err == nil {
			retVal = NewSQInt(i)
		} else {
			err = sqerr.Newf("Unable to Convert %q to an INT", s.Val)
		}
	case tokens.TypeBool:
		switch strings.ToUpper(strings.TrimSpace(s.Val)) {
		case "TRUE":
			retVal = NewSQBool(true)
		case "FALSE":
			retVal = NewSQBool(false)
		default:
			err = sqerr.Newf("Unable to convert string to bool")
		}
	case tokens.TypeFloat:
		f, err = strconv.ParseFloat(s.Val, 64)
		if err == nil {
			retVal = NewSQFloat(f)
		}
	case tokens.TypeString:
		retVal = s
	default:
		err = sqerr.Newf("A value of type %s can not be converted to type %s", s.Type(), newtype)
	}
	return
}

// NewSQString - creates a new SQInt value
func NewSQString(s string) Value {
	return SQString{s}
}

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

//SQNull Methods & Functions ============================================

// ToString - return string representation of type
func (n SQNull) ToString() string {
	return tokens.Null
}

// Type - returns the type
func (n SQNull) Type() string {
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
func (n SQNull) Operation(op string, v Value) (Value, error) {
	return SQNull{}, nil
}

// Convert returns the value converted to the given type
func (n SQNull) Convert(newtype string) (retVal Value, err error) {
	retVal = n
	return
}

// NewSQNull - creates a new SQNull value
func NewSQNull() Value {
	return SQNull{}
}

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

//====================================================================

// CreateValueFromToken - given a token, convert it into a proper Value
func CreateValueFromToken(tkn tokens.Token) (Value, error) {
	var retVal Value

	switch tkn.GetName() {
	case tokens.Num:
		// try to convert to int
		i, err := strconv.Atoi(tkn.GetValue())
		if err != nil {
			//If not Int try to convert to float64
			fp, err := strconv.ParseFloat(tkn.GetValue(), 64)
			if err != nil {
				return nil, sqerr.NewSyntaxf("%q is not a number", tkn.GetValue())
			}
			retVal = NewSQFloat(fp)
		} else {
			retVal = NewSQInt(i)
		}
	case tokens.Quote:
		retVal = NewSQString(tkn.GetValue())
	case tokens.RWTrue:
		retVal = NewSQBool(true)
	case tokens.RWFalse:
		retVal = NewSQBool(false)
	case tokens.Null:
		retVal = NewSQNull()
	default:
		return nil, sqerr.NewInternalf("%q is not a valid Value", tkn.GetString())
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
