package sqbin

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// IntSize is the number of bytes in an Int including it's marker
const IntSize = intSize + markerSize

const intSize = 8
const markerSize = 1

// Type Marker for encoding/decoding to give some extra safety
// Order must be preserved if you need to add a new type so always append
const (
	Uint64Marker = iota + 64
	Int64Marker
	IntMarker
	StringMarker
	ByteMarker
	BoolMarker
	ArrayStringMarker
	ArrayInt64Marker
)

// TypeMarkerStrings translates the marker to a string
var TypeMarkerStrings = map[byte]string{
	Uint64Marker:      "Uint64Marker",
	Int64Marker:       "Int64Marker",
	IntMarker:         "IntMarker",
	StringMarker:      "StringMarker",
	ByteMarker:        "ByteMarker",
	BoolMarker:        "BoolMarker",
	ArrayStringMarker: "ArrayStringMarker",
	ArrayInt64Marker:  "ArrayInt64Marker",
}

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
	c.setTypeMarker(Uint64Marker)
	c.storeIntType(i)
}

//ReadUint64 decodes an uint64 from the codec buffer
func (c *Codec) ReadUint64() uint64 {
	c.getTypeMarker(Uint64Marker)
	return c.getIntType()
}

//WriteInt64 writes an int64 to the codec buffer
func (c *Codec) WriteInt64(i int64) {
	c.setTypeMarker(Int64Marker)
	c.storeIntType(uint64(i))
}

//ReadInt64  decodes an int64 from the codec buffer
func (c *Codec) ReadInt64() int64 {
	c.getTypeMarker(Int64Marker)
	return int64(c.getIntType())
}

//WriteInt writes an int to the codec buffer
func (c *Codec) WriteInt(i int) {
	c.setTypeMarker(IntMarker)
	c.storeIntType(uint64(i))
}

//ReadInt  decodes an int from the codec buffer
func (c *Codec) ReadInt() int {
	c.getTypeMarker(IntMarker)
	return int(c.getIntType())
}

//WriteString writes a string to the codec buffer
func (c *Codec) WriteString(s string) {
	c.setTypeMarker(StringMarker)
	c.WriteInt(len(s))
	c.buff.WriteString(s)
}

//ReadString reads a string from the codec buffer
func (c *Codec) ReadString() string {
	c.getTypeMarker(StringMarker)
	strLen := c.ReadInt()

	str := string(c.buff.Next(strLen))
	if strLen != len(str) {
		panic("Unable to sqbin.ReadString from codec buffer")
	}
	return str
}

// Writebyte writes a byte to the codec buffer
func (c *Codec) Writebyte(b byte) {
	c.setTypeMarker(ByteMarker)
	c.buff.WriteByte(b)
}

//Readbyte reads a byte from the codec buffer
func (c *Codec) Readbyte() byte {
	c.getTypeMarker(ByteMarker)
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
	c.setTypeMarker(BoolMarker)

	if b {
		c.buff.WriteByte(1)
	} else {
		c.buff.WriteByte(0)
	}
}

// ReadBool reads a bool from the codec buffer
func (c *Codec) ReadBool() bool {
	c.getTypeMarker(BoolMarker)
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
	c.setTypeMarker(ArrayStringMarker)
	// write the length first
	c.WriteInt(len(s))

	for _, item := range s {
		c.WriteString(item)
	}
}

// ReadArrayString read a []string from the codec buffer
func (c *Codec) ReadArrayString() []string {
	c.getTypeMarker(ArrayStringMarker)
	l := c.ReadInt()

	strs := make([]string, l)
	for i := 0; i < l; i++ {
		strs[i] = c.ReadString()
	}
	return strs
}

// WriteArrayInt64 writes an []int64 fromthe codec buffer
func (c *Codec) WriteArrayInt64(nArray []int64) {
	c.setTypeMarker(ArrayInt64Marker)
	// write the length first
	c.WriteInt(len(nArray))

	for _, item := range nArray {
		c.WriteInt64(item)
	}
}

// ReadArrayInt64 read an []int64 from the codec buffer
func (c *Codec) ReadArrayInt64() []int64 {
	c.getTypeMarker(ArrayInt64Marker)
	l := c.ReadInt()

	nArray := make([]int64, l)
	for i := 0; i < l; i++ {
		nArray[i] = c.ReadInt64()

	}
	return nArray
}

//InsertInt64 inserts any number of Int64s at the beginning of the buffer
func (c *Codec) InsertInt64(nums ...int64) {
	s := c.buff.String()
	c.buff.Reset()
	for _, num := range nums {
		c.WriteInt64(num)
	}
	c.buff.WriteString(s)
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

func (c *Codec) setTypeMarker(tm byte) {
	c.buff.WriteByte(tm)
}

func (c *Codec) getIntType() uint64 {
	num := c.buff.Next(intSize)
	if len(num) != intSize {
		panic("Unable to getIntType from codec buffer")
	}
	return binary.LittleEndian.Uint64(num)
}

func (c *Codec) getTypeMarker(tm byte) {
	b, err := c.buff.ReadByte()
	if err != nil {
		panic("Unable to sqbin.Readbyte from codec buffer")
	}
	if tm != b {
		panic(fmt.Sprintf("Type marker did not match expected: Actual = %d, Expected = %d", b, tm))
	}
}
