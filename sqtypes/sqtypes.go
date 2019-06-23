package sqtypes

import (
	"fmt"
	"log"
	"reflect"
	"strconv"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	e "github.com/wilphi/sqsrv/sqerr"
	t "github.com/wilphi/sqsrv/tokens"
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
	GetType() string
	GetLen() int
	Equal(v Value) bool
	LessThan(v Value) bool
	GreaterThan(v Value) bool
	IsNull() bool
	Write(c *sqbin.Codec)
	MathOp(op string, v Value) (Value, error)
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

// GetType - returns the type
func (i SQInt) GetType() string {
	return t.TypeInt
}

// GetLen -
func (i SQInt) GetLen() int {
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

// MathOp performs math on two SQInt values based on given operator
func (i SQInt) MathOp(op string, v Value) (Value, error) {
	var res int
	vint, ok := v.(SQInt)
	if !ok {
		if v.IsNull() {
			return v, nil
		}
		return nil, sqerr.New("Type Mismatch: " + v.ToString() + " is not an Int")
	}
	switch op {
	case "+":
		res = i.Val + vint.Val
	case "-":
		res = i.Val - vint.Val
	case "*":
		res = i.Val * vint.Val
	case "/":
		res = i.Val / vint.Val
	case "%":
		res = i.Val % vint.Val
	default:
		return nil, sqerr.NewSyntax("Invalid Operator " + op)
	}
	return NewSQInt(res), nil

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

// GetType - returns the type
func (s SQString) GetType() string {
	return t.TypeString
}

// GetLen -
func (s SQString) GetLen() int {
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

// MathOp performs math on two SQString values based on given operator
func (s SQString) MathOp(op string, v Value) (Value, error) {
	var res string
	vint, ok := v.(SQString)
	if !ok {
		if v.IsNull() {
			return v, nil
		}
		return nil, sqerr.New("Type Mismatch: " + v.ToString() + " is not a String")
	}
	switch op {
	case "+":
		res = s.Val + vint.Val
	default:
		return nil, sqerr.NewSyntax("Invalid Operator " + op)
	}
	return NewSQString(res), nil

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

// GetType - returns the type
func (b SQBool) GetType() string {
	return t.TypeBool
}

// GetLen -
func (b SQBool) GetLen() int {
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

// MathOp is not valid for booleans
func (b SQBool) MathOp(op string, v Value) (Value, error) {
	return nil, sqerr.NewSyntax("Invalid Operation on type Bool")
}

// NewSQBool - creates a new SQBool value
func NewSQBool(b bool) Value {
	return SQBool{b}
}

//SQNull Methods & Functions ============================================

// ToString - return string representation of type
func (n SQNull) ToString() string {
	return t.Null
}

// GetType - returns the type
func (n SQNull) GetType() string {
	return t.Null
}

// GetLen -
func (n SQNull) GetLen() int {
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

// MathOp is always NULL for Null values
func (n SQNull) MathOp(op string, v Value) (Value, error) {
	return SQNull{}, nil
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

// GetType - returns the type
func (fp SQFloat) GetType() string {
	return t.TypeFloat
}

// GetLen -
func (fp SQFloat) GetLen() int {
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

// MathOp performs math on two SQFloat values based on given operator
func (fp SQFloat) MathOp(op string, v Value) (Value, error) {
	var res float64

	// if v is null then the result is null
	if v.IsNull() {
		return v, nil
	}

	vfp, ok := v.(SQFloat)
	if !ok {
		return nil, sqerr.New("Type Mismatch: " + v.ToString() + " is not a Float")
	}
	switch op {
	case "+":
		res = fp.Val + vfp.Val
	case "-":
		res = fp.Val - vfp.Val
	case "*":
		res = fp.Val * vfp.Val
	case "/":
		res = fp.Val / vfp.Val
	default:
		return nil, sqerr.NewSyntax("Invalid Operator " + op + " on type Float")
	}
	return NewSQFloat(res), nil

}

// NewSQFloat - creates a new SQInt value
func NewSQFloat(fp float64) Value {
	return SQFloat{fp}
}

//====================================================================

// CreateValueFromToken - given a token, convert it into a proper Value
func CreateValueFromToken(tkn t.Token) (Value, error) {
	var retVal Value

	switch tkn.GetName() {
	case t.Num:
		// try to convert to int
		i, err := strconv.Atoi(tkn.GetValue())
		if err != nil {
			//If not Int try to convert to float64
			fp, err := strconv.ParseFloat(tkn.GetValue(), 64)
			if err != nil {
				return nil, e.NewSyntax("\"" + tkn.GetValue() + "\" is not a number")
			}
			retVal = NewSQFloat(fp)
		} else {
			retVal = NewSQInt(i)
		}
	case t.Quote:
		retVal = NewSQString(tkn.GetValue())
	case t.RWTrue:
		retVal = NewSQBool(true)
	case t.RWFalse:
		retVal = NewSQBool(false)
	case t.Null:
		retVal = NewSQNull()
	default:
		return nil, e.NewInternal(tkn.GetString() + " is not a valid Value")
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
