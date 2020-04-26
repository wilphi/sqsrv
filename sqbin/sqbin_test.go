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
	binit := []byte{sqbin.IntMarker, 2, 3, 4, 5, 6, 7, 8, 9}
	shortInit := []byte{sqbin.IntMarker, 1, 2, 3, 4}
	data := []dataInt{
		{TestName: "Init Codec", Codec: sqbin.NewCodec(binit), Val: 650777868590383874, Type: sqbin.IntMarker, ExpPanic: "", ReadOp: true, Buffer: binit},
		{TestName: "ReadEmpty Uint64", Codec: encdec, CodecReset: true, Type: sqbin.Uint64Marker, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write Uint64", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.Uint64Marker, ExpPanic: "", ReadOp: false},
		{TestName: "Read Uint64", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.Uint64Marker, ExpPanic: "", ReadOp: true},
		{TestName: "ReadEmpty SQPtr", Codec: encdec, CodecReset: true, Type: sqbin.SQPtrMarker, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write SQPtr", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.SQPtrMarker, ExpPanic: "", ReadOp: false},
		{TestName: "Read SQPtr", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.SQPtrMarker, ExpPanic: "", ReadOp: true},
		{TestName: "ReadEmpty int64", Codec: encdec, CodecReset: true, Type: sqbin.Int64Marker, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write int64", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.Int64Marker, ExpPanic: "", ReadOp: false},
		{TestName: "Read int64", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.Int64Marker, ExpPanic: "", ReadOp: true},
		{TestName: "ReadEmpty int", Codec: encdec, CodecReset: true, Type: sqbin.IntMarker, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write int", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.IntMarker, ExpPanic: "", ReadOp: false},
		{TestName: "Read int", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.IntMarker, ExpPanic: "", ReadOp: true},
		{TestName: "Mismatched Type Marker", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.Int64Marker, ExpPanic: "Type marker did not match expected: Actual = 64-Uint64Marker, Expected = 65-Int64Marker", ReadOp: true, Buffer: []byte{sqbin.Uint64Marker, 1, 2, 3, 4, 5, 6, 7, 8}},
		{TestName: "Buffer Too Short", Codec: sqbin.NewCodec(shortInit), Val: 1234, Type: sqbin.IntMarker, ExpPanic: "Unable to getIntType from codec buffer", ReadOp: true, Buffer: shortInit},
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
		case sqbin.Uint64Marker:
			if d.ReadOp {
				ret = d.Codec.ReadUint64()
			} else {
				d.Codec.WriteUint64(d.Val)
			}
		case sqbin.SQPtrMarker:
			if d.ReadOp {
				ret = uint64(d.Codec.ReadSQPtr())
			} else {
				d.Codec.WriteSQPtr(sqptr.SQPtr(d.Val))
			}
		case sqbin.Int64Marker:
			if d.ReadOp {
				ret = uint64(d.Codec.ReadInt64())
			} else {
				d.Codec.WriteInt64(int64(d.Val))
			}
		case sqbin.IntMarker:
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
	binit := append([]byte{sqbin.FloatMarker}, bits...)
	shortInit := []byte{sqbin.FloatMarker, 1, 2, 3, 4}
	data := []dataFloat{
		{TestName: "Init Codec", Codec: sqbin.NewCodec(binit), Val: 1234.56789, ExpPanic: "", ReadOp: true, Buffer: binit},
		{TestName: "ReadEmpty Float", Codec: encdec, CodecReset: true, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", ReadOp: true},
		{TestName: "Write Float", Codec: encdec, CodecReset: true, Val: 12345, ExpPanic: "", ReadOp: false},
		{TestName: "Read Float", Codec: encdec, CodecReset: false, Val: 12345, ExpPanic: "", ReadOp: true},
		{TestName: "Mismatched Type Marker", Codec: encdec, CodecReset: false, Val: 12345, ExpPanic: "Type marker did not match expected: Actual = 64-Uint64Marker, Expected = 72-FloatMarker", ReadOp: true, Buffer: []byte{sqbin.Uint64Marker, 1, 2, 3, 4, 5, 6, 7, 8}},
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
	bEmpty := []byte{sqbin.StringMarker, sqbin.IntMarker, 0, 0, 0, 0, 0, 0, 0, 0}
	bPartial := []byte{sqbin.StringMarker, sqbin.IntMarker, 9, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bNoInt := []byte{sqbin.StringMarker, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bNoStr := []byte{sqbin.IntMarker, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bGood := []byte{sqbin.StringMarker, sqbin.IntMarker, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}

	var data = []dataString{
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: "Test", ExpPanic: "Unable to sqbin.Readbyte from codec buffer", Function: "READ", Buffer: nil},
		{TestName: "Read Empty String", Codec: encdec, CodecReset: true, Val: "", ExpPanic: "", Function: "READ", Buffer: bEmpty},
		{TestName: "Read Partial String", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: "Unable to sqbin.ReadString from codec buffer", Function: "READ", Buffer: bPartial},
		{TestName: "Read No IntMarker for Len", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: "Type marker did not match expected: Actual = 8-, Expected = 66-IntMarker", Function: "READ", Buffer: bNoInt},
		{TestName: "Read No String Marker", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: "Type marker did not match expected: Actual = 66-IntMarker, Expected = 67-StringMarker", Function: "READ", Buffer: bNoStr},
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
	bPartial := []byte{sqbin.ByteMarker}
	bBadMarker := []byte{sqbin.StringMarker, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bGood := []byte{sqbin.ByteMarker, 123}
	bPeek := []byte{sqbin.ByteMarker, 123, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

	var data = []dataByte{
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: 1, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", Function: "READ", Buffer: nil},
		{TestName: "Read Wrong Marker", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: "Type marker did not match expected: Actual = 67-StringMarker, Expected = 68-ByteMarker", Function: "READ", Buffer: bBadMarker},
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
	bBadMarker := []byte{sqbin.StringMarker, 8, 0, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'}
	bGood := []byte{sqbin.BoolMarker, 1}
	bfalse := []byte{sqbin.BoolMarker, 0}

	var data = []dataBool{
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: true, ExpPanic: "Unable to sqbin.Readbyte from codec buffer", Function: "READ", Buffer: nil},
		{TestName: "Read Wrong Marker", Codec: encdec, CodecReset: true, Val: true, ExpPanic: "Type marker did not match expected: Actual = 67-StringMarker, Expected = 69-BoolMarker", Function: "READ", Buffer: bBadMarker},
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
			ExpPrefix: []byte{sqbin.Int64Marker, 255, 255, 255, 255, 255, 255, 255, 255},
			Values:    []interface{}{int64(-1)},
			ExpPanic:  "",
		},
		{
			TestName:  "Insert One Uint64",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.Uint64Marker, 1, 0, 0, 0, 0, 0, 0, 0},
			Values:    []interface{}{uint64(1)},
			ExpPanic:  "",
		},
		{
			TestName:  "Insert One string",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.StringMarker, 116, 101, 115, 116},
			Values:    []interface{}{"test"},
			ExpPanic:  "unknown type string in sqbin.Insert",
		},
		{
			TestName:  "Insert Uint64,Int64 ",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.Uint64Marker, 1, 0, 0, 0, 0, 0, 0, 0, sqbin.Int64Marker, 255, 255, 255, 255, 255, 255, 255, 255},
			Values:    []interface{}{uint64(1), int64(-1)},
			ExpPanic:  "",
		}, {
			TestName:  "Insert Int64, Uint64",
			TestArray: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			ExpPrefix: []byte{sqbin.Int64Marker, 255, 255, 255, 255, 255, 255, 255, 255, sqbin.Uint64Marker, 1, 0, 0, 0, 0, 0, 0, 0},
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
