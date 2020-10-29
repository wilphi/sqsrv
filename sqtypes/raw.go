package sqtypes

import (
	"fmt"
	"reflect"
)

//Raw is a type that can be converted into sq Values
type Raw interface{}

// RawVals raw values that can be converted to sq Values
// 	used for testing
type RawVals [][]Raw

//RawValue given any type convert it into a SQ Value
// Currently only works for int, string, bool, float32/64
//  nil values get converted to SQNull
func RawValue(raw Raw) Value {
	var retVal Value

	if raw == nil {
		return NewSQNull()
	}
	switch v := raw.(type) {
	case int:
		retVal = NewSQInt(v)
	case string:
		retVal = NewSQString(v)
	case bool:
		retVal = NewSQBool(v)
	case float32:
		retVal = NewSQFloat(float64(v))
	case float64:
		retVal = NewSQFloat(v)
	default:
		panic(fmt.Sprintf("%T is not a valid Raw SQ type", v))
	}
	return retVal
}

//ValueMatrix - converts a 2D array of raw to a 2D array of sqtypes.Value
func (raw RawVals) ValueMatrix() ValueMatrix {
	nRows := len(raw)
	retVals := make(ValueMatrix, nRows)

	for i, row := range raw {
		retVals[i] = CreateValueArrayFromRaw(row)
	}
	return retVals

}

// CreateValuesFromRaw converts a 2D array of raw to a 2D array of sqtypes.Value
func CreateValuesFromRaw(raw RawVals) ValueMatrix {
	nRows := len(raw)
	retVals := make(ValueMatrix, nRows)

	for i, row := range raw {
		retVals[i] = CreateValueArrayFromRaw(row)
	}
	return retVals

}

// CreateValueArrayFromRaw converts an array of raw to an array of sqtypes.Value
func CreateValueArrayFromRaw(rawArray []Raw) ValueArray {
	retVals := make(ValueArray, len(rawArray))
	for j, item := range rawArray {
		retVals[j] = RawValue(item)
	}
	return retVals
}

// Compare2DRaw - returns "" is arrays match otherwise a string describing where the arrays do not match.
func Compare2DRaw(a, b RawVals, aName, bName string) string {
	if len(a) != len(b) {
		return fmt.Sprintf("The number of rows does not match! %s(%d) %s(%d)", aName, len(a), bName, len(b))
	}

	for i := range a {
		if len(a[i]) != len(b[i]) {
			return fmt.Sprintf("The number of cols does not match! %s[%d]-len=%d %s[%d]-len=%d", aName, i, len(a[i]), bName, i, len(b[i])) +
				fmt.Sprintf("\n%s[%d] = %v Does not match %s[%d] = %v", aName, i, a[i], bName, i, b[i])
		}
	}

	for i, row := range a {
		for j, val := range row {
			if reflect.TypeOf(val) != reflect.TypeOf(b[i][j]) {
				return fmt.Sprintf("Type Mismatch: %s[%d][%d] = %T Does not match %s[%d][%d] = %T", aName, i, j, a[i][j], bName, i, j, b[i][j])
			}
			if val != b[i][j] {
				if !(val == nil && b[i][j] == nil) {
					return fmt.Sprintf("%s[%d] = %v Does not match %s[%d] = %v", aName, i, a[i], bName, i, b[i])
				}
			}
		}
	}
	return ""
}
