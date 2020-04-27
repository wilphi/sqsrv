package sqbin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"

	"github.com/wilphi/sqsrv/sqptr"
)

// IntSize is the number of bytes in an Int including it's marker
const IntSize = intSize + markerSize

const intSize = 8
const markerSize = 1

// Type Marker for encoding/decoding to give some extra safety
// Order must be preserved if you need to add a new type so always append
const (
	TMUint64 = iota + 64
	TMInt64
	TMInt
	TMString
	TMByte
	BoolMarker
	TMArrayOfString
	TMArrayOfInt64
	TMFloat
	TMSQPtr
	TMSQPtrs
)

// typeMarkerStrings translates the marker to a string
var typeMarkerStrings = map[TypeMarker]string{
	TMUint64:        "TMUint64",
	TMInt64:         "TMInt64",
	TMInt:           "TMInt",
	TMString:        "TMString",
	TMByte:          "TMByte",
	BoolMarker:      "BoolMarker",
	TMArrayOfString: "TMArrayOfString",
	TMArrayOfInt64:  "TMArrayOfInt64",
	TMFloat:         "TMFloat",
	TMSQPtr:         "TMSQPtr",
	TMSQPtrs:        "TMSQPtrs",
}

// TypeMarker is used to identify a type in a binary representation
type TypeMarker uint8

//TMToString coverts a type marker into it's string
func TMToString(a TypeMarker) string {
	s, ok := typeMarkerStrings[a]
	if !ok {
		s = "Unknown Marker"
	}
	return s
}

type regType struct {
	name   string
	marker TypeMarker
}

//RegisterType allows a complex type to register a Type marker
func RegisterType(name string, marker TypeMarker) {
	a := regType{name: name, marker: marker}
	if marker < 32 {
		log.Panicf("markers (%d) < 32 are reserved for sqbin use", marker)
	}
	m, ok := typeMarkerStrings[marker]
	if ok {
		log.Panicf("marker: %d is already in use as %d-%s", marker, marker, m)
	}
	typeMarkerStrings[marker] = name
	regTypes = append(regTypes, a)
}

// regTypes contains the registered types that apply across all Codecs
var regTypes []regType

// Codec - binary encoding and decoding. All encodings are LittleEndian
type Codec struct {
	buff bytes.Buffer
}

// NewCodec returns a new Codec initialized with the byte array
func NewCodec(p []byte) *Codec {
	c := &Codec{}
	c.Reset()
	c.buff.Write(p)

	return c
}

// WriteUint64 writes a uint64 to the codec buffer
func (c *Codec) WriteUint64(i uint64) {
	c.WriteTypeMarker(TMUint64)
	c.storeIntType(i)
}

//ReadUint64 decodes an uint64 from the codec buffer
func (c *Codec) ReadUint64() uint64 {
	c.ReadTypeMarker(TMUint64)
	return c.getIntType()
}

// WriteSQPtr writes an SQPtr to the codec buffer
func (c *Codec) WriteSQPtr(p sqptr.SQPtr) {
	c.WriteTypeMarker(TMSQPtr)
	c.storeIntType(uint64(p))
}

//ReadSQPtr decodes an SQPtr from the codec buffer
func (c *Codec) ReadSQPtr() sqptr.SQPtr {
	c.ReadTypeMarker(TMSQPtr)
	return sqptr.SQPtr(c.getIntType())
}

//WriteInt64 writes an int64 to the codec buffer
func (c *Codec) WriteInt64(i int64) {
	c.WriteTypeMarker(TMInt64)
	c.storeIntType(uint64(i))
}

//ReadInt64  decodes an int64 from the codec buffer
func (c *Codec) ReadInt64() int64 {
	c.ReadTypeMarker(TMInt64)
	return int64(c.getIntType())
}

//WriteInt writes an int to the codec buffer
func (c *Codec) WriteInt(i int) {
	c.WriteTypeMarker(TMInt)
	c.storeIntType(uint64(i))
}

//ReadInt  decodes an int from the codec buffer
func (c *Codec) ReadInt() int {
	c.ReadTypeMarker(TMInt)
	return int(c.getIntType())
}

//WriteFloat write a float64  to the codec buffer
func (c *Codec) WriteFloat(fp float64) {
	c.WriteTypeMarker(TMFloat)
	c.storeIntType(math.Float64bits(fp))
}

//ReadFloat reads a float64 from the codec buffer
func (c *Codec) ReadFloat() float64 {
	var fp float64
	c.ReadTypeMarker(TMFloat)
	fp = math.Float64frombits(c.getIntType())
	return fp
}

//WriteString writes a string to the codec buffer
func (c *Codec) WriteString(s string) {
	c.WriteTypeMarker(TMString)
	c.WriteInt(len(s))
	c.buff.WriteString(s)
}

//ReadString reads a string from the codec buffer
func (c *Codec) ReadString() string {
	c.ReadTypeMarker(TMString)
	strLen := c.ReadInt()

	str := string(c.buff.Next(strLen))
	if strLen != len(str) {
		panic("Unable to sqbin.ReadString from codec buffer")
	}
	return str
}

// Writebyte writes a byte to the codec buffer
func (c *Codec) Writebyte(b byte) {
	c.WriteTypeMarker(TMByte)
	c.buff.WriteByte(b)
}

//Readbyte reads a byte from the codec buffer
func (c *Codec) Readbyte() byte {
	c.ReadTypeMarker(TMByte)
	b, err := c.buff.ReadByte()
	if err != nil {
		panic("Unable to sqbin.Readbyte from codec buffer")
	}
	return b
}

// PeekByte returns the value of a byte from the codec buffer without reading it
func (c *Codec) PeekByte() byte {
	s := c.buff.String()
	b := c.Readbyte()
	c.buff.Reset()
	c.buff.WriteString(s)
	return b
}

// WriteBool writes a bool to the codec buffer
func (c *Codec) WriteBool(b bool) {
	c.WriteTypeMarker(BoolMarker)

	if b {
		c.buff.WriteByte(1)
	} else {
		c.buff.WriteByte(0)
	}
}

// ReadBool reads a bool from the codec buffer
func (c *Codec) ReadBool() bool {
	c.ReadTypeMarker(BoolMarker)
	b, err := c.buff.ReadByte()
	if err != nil {
		panic("Unable to sqbin.ReadBool from codec buffer")
	}
	return b == 1
}

// Len returns the length of the codec buffer
func (c *Codec) Len() int {
	return c.buff.Len()
}

//Bytes returns the current codec buffer as []byte
func (c *Codec) Bytes() []byte {
	return c.buff.Bytes()
}

// Reset resets the current codec buffer to empty
func (c *Codec) Reset() {
	c.buff.Reset()
}

// Write adds a byte slice to the codec buffer
func (c *Codec) Write(p []byte) {
	c.buff.Write(p)
}

//WriteArrayString writes a []string to the codec buffer
func (c *Codec) WriteArrayString(s []string) {
	c.WriteTypeMarker(TMArrayOfString)
	// write the length first
	c.WriteInt(len(s))

	for _, item := range s {
		c.WriteString(item)
	}
}

// ReadArrayString read a []string from the codec buffer
func (c *Codec) ReadArrayString() []string {
	c.ReadTypeMarker(TMArrayOfString)
	l := c.ReadInt()

	strs := make([]string, l)
	for i := 0; i < l; i++ {
		strs[i] = c.ReadString()
	}
	return strs
}

// WriteArrayInt64 writes an []int64 to the codec buffer
func (c *Codec) WriteArrayInt64(nArray []int64) {
	c.WriteTypeMarker(TMArrayOfInt64)
	// write the length first
	c.WriteInt(len(nArray))

	for _, item := range nArray {
		c.WriteInt64(item)
	}
}

// ReadArrayInt64 read an []int64 from the codec buffer
func (c *Codec) ReadArrayInt64() []int64 {
	c.ReadTypeMarker(TMArrayOfInt64)
	l := c.ReadInt()

	nArray := make([]int64, l)
	for i := 0; i < l; i++ {
		nArray[i] = c.ReadInt64()

	}
	return nArray
}

// WriteSQPtrs writes an []SQPtr to the codec buffer
func (c *Codec) WriteSQPtrs(nArray sqptr.SQPtrs) {
	c.WriteTypeMarker(TMSQPtrs)
	// write the length first
	c.WriteInt(len(nArray))

	for _, item := range nArray {
		c.WriteSQPtr(item)
	}
}

// ReadSQPtrs read an []SQPtr from the codec buffer
func (c *Codec) ReadSQPtrs() sqptr.SQPtrs {
	c.ReadTypeMarker(TMSQPtrs)
	l := c.ReadInt()

	nArray := make(sqptr.SQPtrs, l)
	for i := 0; i < l; i++ {
		nArray[i] = c.ReadSQPtr()

	}
	return nArray
}

// Insert inserts valid types at the beginning of the buffer
func (c *Codec) Insert(vars ...interface{}) {
	s := c.buff.String()
	c.buff.Reset()
	size := len(s)
	//Figure out the size
	for _, vr := range vars {
		switch vr.(type) {
		case int64:
			size += 9
		case uint64:
			size += 9
		default:
			log.Panicf("unknown type %T in sqbin.Insert", vr)
		}
	}
	// make sure buffer is sized correctly
	c.buff.Grow(size)

	for _, vr := range vars {
		switch v := vr.(type) {
		case int64:
			c.WriteInt64(v)
		case uint64:
			c.WriteUint64(v)
		default:
			log.Panicf("unknown type %T in sqbin.Insert", v)
		}
	}

	c.buff.WriteString(s)

}

// WriteTypeMarker writes the given TypeMarker to the Codec
func (c *Codec) WriteTypeMarker(tm TypeMarker) {
	c.buff.WriteByte(byte(tm))
}

// ReadTypeMarker reads the Codec to ensure that the next byte is the given TypeMarker
func (c *Codec) ReadTypeMarker(tm TypeMarker) {
	b, err := c.buff.ReadByte()
	if err != nil {
		panic("Unable to sqbin.Readbyte from codec buffer")
	}
	if byte(tm) != b {
		panic(fmt.Sprintf("Type marker did not match expected: Actual = %d-%s, Expected = %d-%s", b, TMToString(TypeMarker(b)), tm, TMToString(tm)))
	}
}

// PeekTypeMarker returns the first byte in the buffer expecting that it is a TypeMarker
func (c *Codec) PeekTypeMarker() TypeMarker {
	b, err := c.buff.ReadByte()
	if err != nil {
		panic("Unable to sqbin.Readbyte from codec buffer: " + err.Error())
	}
	c.buff.UnreadByte()
	return TypeMarker(b)
}

////////////////////////////////////////////////////////////////////////////////////////
// Private Functions
////////////////////////////////////////////////////////////////////////////////////////

// storeIntType is a helper function to store 8 byte integer values
// ie uint64, int, int64
func (c *Codec) storeIntType(i uint64) {
	store := make([]byte, 8)
	binary.LittleEndian.PutUint64(store, i)
	c.buff.Write(store)
}

func (c *Codec) getIntType() uint64 {
	num := c.buff.Next(intSize)
	if len(num) != intSize {
		panic("Unable to getIntType from codec buffer")
	}
	return binary.LittleEndian.Uint64(num)
}
