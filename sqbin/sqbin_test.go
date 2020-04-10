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
	ExpPanic   bool
	ReadOp     bool
	Buffer     []byte
}

func TestIntegers(t *testing.T) {
	encdec := sqbin.NewCodec(nil)
	binit := []byte{sqbin.IntMarker, 2, 3, 4, 5, 6, 7, 8, 9}
	shortInit := []byte{sqbin.IntMarker, 1, 2, 3, 4}
	data := []dataInt{
		{TestName: "Init Codec", Codec: sqbin.NewCodec(binit), Val: 650777868590383874, Type: sqbin.IntMarker, ExpPanic: false, ReadOp: true, Buffer: binit},
		{TestName: "ReadEmpty Uint64", Codec: encdec, CodecReset: true, Type: sqbin.Uint64Marker, ExpPanic: true, ReadOp: true},
		{TestName: "Write Uint64", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.Uint64Marker, ExpPanic: false, ReadOp: false},
		{TestName: "Read Uint64", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.Uint64Marker, ExpPanic: false, ReadOp: true},
		{TestName: "ReadEmpty SQPtr", Codec: encdec, CodecReset: true, Type: sqbin.SQPtrMarker, ExpPanic: true, ReadOp: true},
		{TestName: "Write SQPtr", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.SQPtrMarker, ExpPanic: false, ReadOp: false},
		{TestName: "Read SQPtr", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.SQPtrMarker, ExpPanic: false, ReadOp: true},
		{TestName: "ReadEmpty int64", Codec: encdec, CodecReset: true, Type: sqbin.Int64Marker, ExpPanic: true, ReadOp: true},
		{TestName: "Write int64", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.Int64Marker, ExpPanic: false, ReadOp: false},
		{TestName: "Read int64", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.Int64Marker, ExpPanic: false, ReadOp: true},
		{TestName: "ReadEmpty int", Codec: encdec, CodecReset: true, Type: sqbin.IntMarker, ExpPanic: true, ReadOp: true},
		{TestName: "Write int", Codec: encdec, CodecReset: true, Val: 12345, Type: sqbin.IntMarker, ExpPanic: false, ReadOp: false},
		{TestName: "Read int", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.IntMarker, ExpPanic: false, ReadOp: true},
		{TestName: "Mismatched Type Marker", Codec: encdec, CodecReset: false, Val: 12345, Type: sqbin.Int64Marker, ExpPanic: true, ReadOp: true, Buffer: []byte{sqbin.Uint64Marker, 1, 2, 3, 4, 5, 6, 7, 8}},
		{TestName: "Buffer Too Short", Codec: sqbin.NewCodec(shortInit), Val: 1234, Type: sqbin.IntMarker, ExpPanic: true, ReadOp: true, Buffer: shortInit},
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
	ExpPanic   bool
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
		{TestName: "Init Codec", Codec: sqbin.NewCodec(binit), Val: 1234.56789, ExpPanic: false, ReadOp: true, Buffer: binit},
		{TestName: "ReadEmpty Float", Codec: encdec, CodecReset: true, ExpPanic: true, ReadOp: true},
		{TestName: "Write Float", Codec: encdec, CodecReset: true, Val: 12345, ExpPanic: false, ReadOp: false},
		{TestName: "Read Float", Codec: encdec, CodecReset: false, Val: 12345, ExpPanic: false, ReadOp: true},
		{TestName: "Mismatched Type Marker", Codec: encdec, CodecReset: false, Val: 12345, ExpPanic: true, ReadOp: true, Buffer: []byte{sqbin.Uint64Marker, 1, 2, 3, 4, 5, 6, 7, 8}},
		{TestName: "Buffer Too Short", Codec: sqbin.NewCodec(shortInit), Val: 1234, ExpPanic: true, ReadOp: true, Buffer: shortInit},
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
	ExpPanic   bool
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
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: "Test", ExpPanic: true, Function: "READ", Buffer: nil},
		{TestName: "Read Empty String", Codec: encdec, CodecReset: true, Val: "", ExpPanic: false, Function: "READ", Buffer: bEmpty},
		{TestName: "Read Partial String", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: true, Function: "READ", Buffer: bPartial},
		{TestName: "Read No IntMarker for Len", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: true, Function: "READ", Buffer: bNoInt},
		{TestName: "Read No String Marker", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: true, Function: "READ", Buffer: bNoStr},
		{TestName: "Read String", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: false, Function: "READ", Buffer: bGood},
		{TestName: "Write String", Codec: encdec, CodecReset: true, Val: "abcdefgh", ExpPanic: false, Function: "WRITE", Buffer: bGood},
		{TestName: "Write Empty String", Codec: encdec, CodecReset: true, Val: "", ExpPanic: false, Function: "WRITE", Buffer: bEmpty},
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
	ExpPanic   bool
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
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: 1, ExpPanic: true, Function: "READ", Buffer: nil},
		{TestName: "Read Wrong Marker", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: true, Function: "READ", Buffer: bBadMarker},
		{TestName: "Read Marker no Byte", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: true, Function: "READ", Buffer: bPartial},
		{TestName: "Read Byte", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: false, Function: "READ", Buffer: bGood},
		{TestName: "Write Byte", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: false, Function: "WRITE", Buffer: bGood},
		{TestName: "Peek Byte", Codec: encdec, CodecReset: true, Val: 123, ExpPanic: false, Function: "PEEK", Buffer: bPeek},
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
	ExpPanic   bool
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
		{TestName: "Read Empty Buffer", Codec: encdec, CodecReset: true, Val: true, ExpPanic: true, Function: "READ", Buffer: nil},
		{TestName: "Read Wrong Marker", Codec: encdec, CodecReset: true, Val: true, ExpPanic: true, Function: "READ", Buffer: bBadMarker},
		{TestName: "Read Marker no Bool", Codec: encdec, CodecReset: true, Val: true, ExpPanic: true, Function: "READ", Buffer: bPartial},
		{TestName: "Read Bool", Codec: encdec, CodecReset: true, Val: true, ExpPanic: false, Function: "READ", Buffer: bGood},
		{TestName: "Write Bool true", Codec: encdec, CodecReset: true, Val: true, ExpPanic: false, Function: "WRITE", Buffer: bGood},
		{TestName: "Write Bool false", Codec: encdec, CodecReset: true, Val: false, ExpPanic: false, Function: "WRITE", Buffer: bfalse},
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
func TestInsertInt64(t *testing.T) {
	testArray := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	encdec := sqbin.NewCodec(testArray)

	encdec.InsertInt64(1)
	if len(testArray)+9 != encdec.Len() {
		t.Error("Int64 was not added to codec")
	}

	finalArray := []byte{sqbin.Int64Marker, 1, 0, 0, 0, 0, 0, 0, 0}
	finalArray = append(finalArray, testArray...)
	if !bytes.Equal(finalArray, encdec.Bytes()) {
		t.Error("Expected results do not match actual results")
	}
}
