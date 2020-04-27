package sqbin_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtest"
)

//////////////////////////////////////////////////////////////////////////////////////
// Int tests
type dataInt struct {
	TestName   string
	Codec      *sqbin.Codec
	CodecReset bool
	Val        uint64
	Type       byte
	ExpPanic   string
	ReadOp     bool
	Buffer     []byte
}

func TestIntegers(t *testing.T) {
	encdec := sqbin.NewCodec(nil)
	binit := []byte{sqbin.TMInt, 2, 3, 4, 5, 6, 7, 8, 9}
	shortInit := []byte{sqbin.TMInt, 1, 2, 3, 4}
	data := []dataInt{
		{TestName: "Init Codec", Codec: sqbin.NewCodec(binit), Val: 650777868590383874, Type: sqbin.TMInt, ExpPanic: "", ReadOp: true, Buffer: binit},
		{TestName: "ReadEmpty Uint64", Codec: encdec, CodecReset: true, Type: sqbin.TMUint64, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write Uint64", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.TMUint64, ExpPanic: "", ReadOp: false},
		{TestName: "Read Uint64", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.TMUint64, ExpPanic: "", ReadOp: true},
		{TestName: "ReadEmpty SQPtr", Codec: encdec, CodecReset: true, Type: sqbin.TMSQPtr, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write SQPtr", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.TMSQPtr, ExpPanic: "", ReadOp: false},
		{TestName: "Read SQPtr", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.TMSQPtr, ExpPanic: "", ReadOp: true},
		{TestName: "ReadEmpty int64", Codec: encdec, CodecReset: true, Type: sqbin.TMInt64, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write int64", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.TMInt64, ExpPanic: "", ReadOp: false},
		{TestName: "Read int64", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.TMInt64, ExpPanic: "", ReadOp: true},
		{TestName: "ReadEmpty int", Codec: encdec, CodecReset: true, Type: sqbin.TMInt, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write int", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.TMInt, ExpPanic: "", ReadOp: false},
		{TestName: "Read int", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.TMInt, ExpPanic: "", ReadOp: true},
		{TestName: "Mismatched Type Marker", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.TMInt64, ExpPanic: "Type marker did not match expected: Actual = 64-TMUint64, Expected = 65-TMInt64", ReadOp: true, Buffer: []byte{sqbin.TMUint64, 1, 2, 3, 4, 5, 6, 7, 8}},
		{TestName: "Buffer Too Short", Codec: sqbin.NewCodec(shortInit), Val: 1234, Type: sqbin.TMInt, ExpPanic: "Unable to getIntType from codec buffer", ReadOp: true, Buffer: shortInit},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testIntTypesFunc(row))

	}
}

func testIntTypesFunc(d dataInt) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)
		//log.Warn(">>>" + d.TestName)
		if d.CodecReset {
			d.Codec.Reset()
		}
		if d.ReadOp && d.Buffer != nil {
			d.Codec.Reset()
			d.Codec.Write(d.Buffer)
		}
		var ret uint64
		switch d.Type {
		case sqbin.TMUint64:
			if d.ReadOp {
				ret = d.Codec.ReadUint64()
			} else {
				d.Codec.WriteUint64(d.Val)
			}
		case sqbin.TMSQPtr:
			if d.ReadOp {
				ret = uint64(d.Codec.ReadSQPtr())
			} else {
				d.Codec.WriteSQPtr(sqptr.SQPtr(d.Val))
			}
		case sqbin.TMInt64:
			if d.ReadOp {
				ret = uint64(d.Codec.ReadInt64())
			} else {
				d.Codec.WriteInt64(int64(d.Val))
			}
		case sqbin.TMInt:
			if d.ReadOp {
				ret = uint64(d.Codec.ReadInt())
			} else {
				d.Codec.WriteInt(int(d.Val))
			}
		default:
			t.Errorf("Type is invalid: %d", d.Type)
		}

		if d.ReadOp && ret != d.Val {
			t.Errorf("Actual Value %d does not match expected value %d", ret, d.Val)
		}
		if !d.ReadOp && d.Buffer != nil {
			if !bytes.Equal(d.Buffer, d.Codec.Bytes()) {
				t.Errorf("Expect state of buffer does not match current buffer")
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Float tests
type dataFloat struct {
	TestName   string
	Codec      *sqbin.Codec
	CodecReset bool
	Val        float64
	ExpPanic   string
	ReadOp     bool
	Buffer     []byte
}

func TestFloats(t *testing.T) {
	encdec := sqbin.NewCodec(nil)
	bits := make([]byte, 8)
	binary.LittleEndian.PutUint64(bits, math.Float64bits(1234.56789))
	binit := append([]byte{sqbin.TMFloat}, bits...)
	shortInit := []byte{sqbin.TMFloat, 1, 2, 3, 4}
	data := []dataFloat{
		{TestName: "Init Codec", Codec: sqbin.NewCodec(binit), Val: 1234.56789, ExpPanic: "", ReadOp: true, Buffer: binit},
		{TestName: "ReadEmpty Float", Codec: encdec, CodecReset: true, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write Float", Codec: encdec, CodecReset: true, Val: 12345, ExpPanic: "", ReadOp: false},
		{TestName: "Read Float", Codec: encdec, CodecReset: false, Val: 12345, ExpPanic: "", ReadOp: true},
		{TestName: "Mismatched Type Marker", Codec: encdec, CodecReset: false, Val: 12345, ExpPanic: "Type marker did not match expected: Actual = 64-TMUint64, Expected = 72-TMFloat", ReadOp: true, Buffer: []byte{sqbin.TMUint64, 1, 2, 3, 4, 5, 6, 7, 8}},
		{TestName: "Buffer Too Short", Codec: sqbin.NewCodec(shortInit), Val: 1234, ExpPanic: "Unable to getIntType from codec buffer", ReadOp: true, Buffer: shortInit},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testFloatFunc(row))

	}
}

func testFloatFunc(d dataFloat) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		//log.Warn(">>>" + d.TestName)
		if d.CodecReset {
			d.Codec.Reset()
		}
		if d.ReadOp && d.Buffer != nil {
			d.Codec.Reset()
			d.Codec.Write(d.Buffer)
		}
		var ret float64
		if d.ReadOp {
			ret = d.Codec.ReadFloat()
		} else {
			d.Codec.WriteFloat(d.Val)
		}

		if d.ReadOp && ret != d.Val {
			t.Errorf("Actual Value %f does not match expected value %f", ret, d.Val)
		}
		if !d.ReadOp && d.Buffer != nil {
			if !bytes.Equal(d.Buffer, d.Codec.Bytes()) {
				t.Errorf("Expect state of buffer does not match current buffer")
			}
		}
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////
// String tests
type dataString struct {
	TestName   string
	Codec      *sqbin.Codec
	CodecReset bool
	Val        string
	ExpPanic   string
	Function   string
	Buffer     []byte
}

func TestString(t *testing.T) {
	encdec := sqbin.NewCodec(nil)
	bEmpty := []byte{sqbin.TMString, sqbin.TMInt, 0, 0, 0, 0, 0, 0, 0, 0}
	bPartial := []byte{sqbin.TMString, sqbin.TMInt, 9, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bNoInt := []byte{sqbin.TMString, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bNoStr := []byte{sqbin.TMInt, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bGood := []byte{sqbin.TMString, sqbin.TMInt, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}

	var data = []dataString{
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: "Test", ExpPanic: "Unable to sqbin.Readbyte from codec buffer", Function: "READ", Buffer: nil},
		{TestName: "Read Empty String", Codec: encdec, CodecReset: true, Val: "", ExpPanic: "", Function: "READ", Buffer: bEmpty},
		{TestName: "Read Partial String", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: "Unable to sqbin.ReadString from codec buffer", Function: "READ", Buffer: bPartial},
		{TestName: "Read No TMInt for Len", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: "Type marker did not match expected: Actual = 8-Unknown Marker, Expected = 66-TMInt", Function: "READ", Buffer: bNoInt},
		{TestName: "Read No String Marker", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: "Type marker did not match expected: Actual = 66-TMInt, Expected = 67-TMString", Function: "READ", Buffer: bNoStr},
		{TestName: "Read String", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: "", Function: "READ", Buffer: bGood},
		{TestName: "Write String", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: "", Function: "WRITE", Buffer: bGood},
		{TestName: "Write Empty String", Codec: encdec, CodecReset: true, Val: "", ExpPanic: "", Function: "WRITE", Buffer: bEmpty},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testStrTypeFunc(row))

	}
}

func testStrTypeFunc(d dataString) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		//log.Warn(">>>" + d.TestName)
		if d.CodecReset {
			d.Codec.Reset()
		}
		if d.Function == "READ" && d.Buffer != nil {
			d.Codec.Reset()
			d.Codec.Write(d.Buffer)
		}
		var ret string
		switch d.Function {
		case "READ":
			ret = d.Codec.ReadString()
		case "WRITE":
			d.Codec.WriteString(d.Val)
		default:
			t.Errorf("Function Type is invalid: %s", d.Function)
		}

		if d.Function == "READ" && ret != d.Val {
			t.Errorf("Actual Value %s does not match expected value %s", ret, d.Val)
		}
		if d.Function == "WRITE" && d.Buffer != nil {
			if len(d.Buffer) != d.Codec.Len() {
				t.Errorf("The given buffer and the codec.Len are different")
			}
			if !bytes.Equal(d.Buffer, d.Codec.Bytes()) {
				t.Errorf("Expect state of buffer does not match current buffer")
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////
// Byte tests

type dataByte struct {
	TestName   string
	Codec      *sqbin.Codec
	CodecReset bool
	Val        byte
	ExpPanic   string
	Function   string
	Buffer     []byte
}

func TestByte(t *testing.T) {
	encdec := sqbin.NewCodec(nil)
	bPartial := []byte{sqbin.TMByte}
	bBadMarker := []byte{sqbin.TMString, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bGood := []byte{sqbin.TMByte, 123}
	bPeek := []byte{sqbin.TMByte, 123, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

	var data = []dataByte{
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: 1, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", Function: "READ", Buffer: nil},
		{TestName: "Read Wrong Marker", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: "Type marker did not match expected: Actual = 67-TMString, Expected = 68-TMByte", Function: "READ", Buffer: bBadMarker},
		{TestName: "Read Marker no Byte", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", Function: "READ", Buffer: bPartial},
		{TestName: "Read Byte", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: "", Function: "READ", Buffer: bGood},
		{TestName: "Write Byte", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: "", Function: "WRITE", Buffer: bGood},
		{TestName: "Peek Byte", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: "", Function: "PEEK", Buffer: bPeek},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testByteTypeFunc(row))

	}
}

func testByteTypeFunc(d dataByte) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		//log.Warn(">>>" + d.TestName)
		if d.CodecReset {
			d.Codec.Reset()
		}
		if (d.Function == "READ" || d.Function == "PEEK") && d.Buffer != nil {
			d.Codec.Reset()
			d.Codec.Write(d.Buffer)
		}
		var ret byte
		switch d.Function {
		case "READ":
			ret = d.Codec.Readbyte()
		case "WRITE":
			d.Codec.Writebyte(d.Val)
		case "PEEK":
			initial := make([]byte, d.Codec.Len())
			copy(initial, d.Codec.Bytes())
			ret = d.Codec.PeekByte()
			result := make([]byte, d.Codec.Len())
			copy(result, d.Codec.Bytes())
			if !reflect.DeepEqual(initial, result) {
				t.Error("PeekByte changes the buffer")
			}
		default:
			t.Errorf("Function Type is invalid: %s", d.Function)
		}

		if (d.Function == "READ" || d.Function == "PEEK") && ret != d.Val {
			t.Errorf("Actual Value %d does not match expected value %d", ret, d.Val)
		}
		if d.Function == "WRITE" && d.Buffer != nil {
			if !bytes.Equal(d.Buffer, d.Codec.Bytes()) {
				t.Errorf("Expect state of buffer does not match current buffer")
			}
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// Bool tests

type dataBool struct {
	TestName   string
	Codec      *sqbin.Codec
	CodecReset bool
	Val        bool
	ExpPanic   string
	Function   string
	Buffer     []byte
}

func TestBool(t *testing.T) {
	encdec := sqbin.NewCodec(nil)
	bPartial := []byte{sqbin.BoolMarker}
	bBadMarker := []byte{sqbin.TMString, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bGood := []byte{sqbin.BoolMarker, 1}
	bfalse := []byte{sqbin.BoolMarker, 0}

	var data = []dataBool{
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: true, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", Function: "READ", Buffer: nil},
		{TestName: "Read Wrong Marker", Codec: encdec, CodecReset: true, Val: true, ExpPanic: "Type marker did not match expected: Actual = 67-TMString, Expected = 69-BoolMarker", Function: "READ", Buffer: bBadMarker},
		{TestName: "Read Marker no Bool", Codec: encdec, CodecReset: true, Val: true, ExpPanic: "Unable to sqbin.ReadBool from codec buffer", Function: "READ", Buffer: bPartial},
		{TestName: "Read Bool", Codec: encdec, CodecReset: true, Val: true, ExpPanic: "", Function: "READ", Buffer: bGood},
		{TestName: "Write Bool true", Codec: encdec, CodecReset: true, Val: true, ExpPanic: "", Function: "WRITE", Buffer: bGood},
		{TestName: "Write Bool false", Codec: encdec, CodecReset: true, Val: false, ExpPanic: "", Function: "WRITE", Buffer: bfalse},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testBoolTypeFunc(row))

	}
}

func testBoolTypeFunc(d dataBool) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		//log.Warn(">>>" + d.TestName)
		if d.CodecReset {
			d.Codec.Reset()
		}
		if d.Function == "READ" && d.Buffer != nil {
			d.Codec.Reset()
			d.Codec.Write(d.Buffer)
		}
		var ret bool
		switch d.Function {
		case "READ":
			ret = d.Codec.ReadBool()
		case "WRITE":
			d.Codec.WriteBool(d.Val)
		default:
			t.Errorf("Function Type is invalid: %s", d.Function)
		}

		if d.Function == "READ" && ret != d.Val {
			t.Errorf("Actual Value %t does not match expected value %t", ret, d.Val)
		}
		if d.Function == "WRITE" && d.Buffer != nil {
			if !bytes.Equal(d.Buffer, d.Codec.Bytes()) {
				t.Errorf("Expect state of buffer does not match current buffer")
			}
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////
// Array tests

func TestStringArray(t *testing.T) {
	encdec := sqbin.NewCodec(nil)

	warray := []string{
		"test One",
		"Test 2",
		"test3",
	}
	encdec.WriteArrayString(warray)
	rarray := encdec.ReadArrayString()
	if !reflect.DeepEqual(warray, rarray) {
		t.Error("The Written array does not match the Read array")
	}

}

func TestInt64Array(t *testing.T) {
	encdec := sqbin.NewCodec(nil)

	warray := []int64{1, 2, 3, 4, 5, 6, 7, 123456789012345}
	encdec.WriteArrayInt64(warray)
	rarray := encdec.ReadArrayInt64()
	if !reflect.DeepEqual(warray, rarray) {
		t.Error("The Written array does not match the Read array")
	}

}

func TestSQPtrsArray(t *testing.T) {
	encdec := sqbin.NewCodec(nil)

	warray := sqptr.SQPtrs{1, 2, 3, 4, 5, 6, 7, 123456789012345}
	encdec.WriteSQPtrs(warray)
	rarray := encdec.ReadSQPtrs()
	if !reflect.DeepEqual(warray, rarray) {
		t.Error("The Written array does not match the Read array")
	}

}
func TestInsert(t *testing.T) {

	data := []dataInsert{
		{
			TestName:  "Insert One Int64",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.TMInt64, 255, 255, 255, 255, 255, 255, 255, 255},
			Values:    []interface{}{int64(-1)},
			ExpPanic:  "",
		},
		{
			TestName:  "Insert One Uint64",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.TMUint64, 1, 0, 0, 0, 0, 0, 0, 0},
			Values:    []interface{}{uint64(1)},
			ExpPanic:  "",
		},
		{
			TestName:  "Insert One string",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.TMString, 116, 101, 115, 116},
			Values:    []interface{}{"test"},
			ExpPanic:  "unknown type string in sqbin.Insert",
		},
		{
			TestName:  "Insert Uint64,Int64 ",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.TMUint64, 1, 0, 0, 0, 0, 0, 0, 0, sqbin.TMInt64, 255, 255, 255, 255, 255, 255, 255, 255},
			Values:    []interface{}{uint64(1), int64(-1)},
			ExpPanic:  "",
		}, {
			TestName:  "Insert Int64, Uint64",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.TMInt64, 255, 255, 255, 255, 255, 255, 255, 255, sqbin.TMUint64, 1, 0, 0, 0, 0, 0, 0, 0},
			Values:    []interface{}{int64(-1), uint64(1)},
			ExpPanic:  "",
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testInsertFunc(row))

	}
}

type dataInsert struct {
	TestName  string
	TestArray []byte
	ExpPrefix []byte
	ExpLen    int
	Values    []interface{}
	ExpPanic  string
}

func testInsertFunc(d dataInsert) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		encdec := sqbin.NewCodec(d.TestArray)
		encdec.Insert(d.Values...)

		ExpArray := append(d.ExpPrefix, d.TestArray...)

		if !bytes.Equal(ExpArray, encdec.Bytes()) {
			t.Error("Expected results do not match actual results")
			t.Errorf("Expected: %v", ExpArray)
			t.Errorf("  Actual: %v)", encdec.Bytes())
			return
		}
	}
}

////////////////////

type regtest struct {
	A string
}

func (r *regtest) Encode(enc *sqbin.Codec) {
	enc.WriteString(r.A)

}
func (r *regtest) Decode(dec *sqbin.Codec) {
	r.A = dec.ReadString()
}

type RegisterData struct {
	TestName   string
	Codec      *sqbin.Codec
	ExpPanic   string
	Marker     sqbin.TypeMarker
	DoubleReg  bool
	FakeMarker byte
	Name       string
}

func TestRegisterType(t *testing.T) {
	data := []RegisterData{
		{
			TestName: "RegTest Marker",
			ExpPanic: "",
			Marker:   252,
			Name:     "RegTestMarker",
		},
		{
			TestName: "RegTest Marker",
			ExpPanic: "markers (1) < 32 are reserved for sqbin use",
			Marker:   1,
			Name:     "RegTestMarker",
		},
		{
			TestName:  "RegTest Marker",
			ExpPanic:  "marker: 252 is already in use as 252-RegTestMarker",
			Marker:    252,
			Name:      "RegTestMarker",
			DoubleReg: true,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testRegisterFunc(row))

	}
}

func testRegisterFunc(d RegisterData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		sqbin.RegisterType(d.Name, d.Marker)

		if d.DoubleReg {
			sqbin.RegisterType("DoubleTEST", d.Marker)

		}
		if sqbin.TMToString(d.Marker) != d.Name {
			t.Errorf("Expected Marker %d-%s does not match actual %d-%s", d.Marker, d.Name, d.Marker, sqbin.TMToString(d.Marker))
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////
// Marker tests
//

type MarkerData struct {
	TestName   string
	ExpPanic   string
	Marker     sqbin.TypeMarker
	UseFake    bool
	FakeMarker sqbin.TypeMarker
}

func TestMarkers(t *testing.T) {
	data := []MarkerData{
		{
			TestName: "TMInt",
			ExpPanic: "",
			Marker:   sqbin.TMInt,
		},
		{
			TestName:   "TMInt Wrong Marker",
			ExpPanic:   "Type marker did not match expected: Actual = 67-TMString, Expected = 66-TMInt",
			Marker:     sqbin.TMInt,
			UseFake:    true,
			FakeMarker: sqbin.TMString,
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testMarkerFunc(row))

	}
}

func testMarkerFunc(d MarkerData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)

		cdr := sqbin.NewCodec(nil)
		if d.UseFake {
			cdr.WriteTypeMarker(d.FakeMarker)
		} else {
			cdr.WriteTypeMarker(d.Marker)
		}

		pm := cdr.PeekTypeMarker()
		if pm != d.Marker && !d.UseFake {
			t.Errorf("Expected Marker %d-%s does not match Peek actual %d-%s", d.Marker, sqbin.TMToString(d.Marker), pm, sqbin.TMToString(pm))
		}

		cdr.ReadTypeMarker(d.Marker)
	}
}

////////////////////
