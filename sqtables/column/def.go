package column

import (
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/tokens"
)

// Def - column definition
type Def struct {
	ColName   string
	ColType   tokens.TokenID
	Idx       int
	IsNotNull bool
	TableName string
}

// NewDef -
func NewDef(colName string, colType tokens.TokenID, isNotNull bool) Def {
	return Def{ColName: colName, ColType: colType, Idx: -1, IsNotNull: isNotNull}
}

// String returns a string representation of the Def
func (c *Def) String() string {
	var ntype string

	if c.IsNotNull {
		ntype = " NOT NULL"
	}

	ret := "{" + c.ColName + ", " + tokens.IDName(c.ColType) + ntype + "}"
	return ret
}

// Ref makes a column.Ref to the column.Def
func (c *Def) Ref() Ref {
	return Ref{ColName: c.ColName, ColType: c.ColType, Idx: c.Idx, IsNotNull: c.IsNotNull, TableName: c.TableName}
}

//Encode outputs a binary encoded version of the Def to the codec
func (c *Def) Encode(enc *sqbin.Codec) {

	enc.WriteString(c.ColName)
	enc.WriteUint64(uint64(c.ColType))
	enc.WriteInt(c.Idx)
	enc.WriteBool(c.IsNotNull)
	enc.WriteString(c.TableName)

}

//Decode a binary encoded version of a Def from the codec
func (c *Def) Decode(dec *sqbin.Codec) {

	c.ColName = dec.ReadString()
	c.ColType = tokens.TokenID(dec.ReadUint64())
	c.Idx = dec.ReadInt()
	c.IsNotNull = dec.ReadBool()
	c.TableName = dec.ReadString()
}
