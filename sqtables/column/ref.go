package column

import (
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/tokens"
)

// Ref - reference to a column definition
type Ref struct {
	ColName          string
	ColType          tokens.TokenID
	Idx              int
	IsNotNull        bool
	TableName        string
	TableAlias       string
	DisplayTableName bool
}

// NewRef creates a new column reference
func NewRef(colName string, colType tokens.TokenID, isNotNull bool) Ref {
	return Ref{ColName: colName, ColType: colType, Idx: -1, IsNotNull: isNotNull, DisplayTableName: false}
}

// String returns a string representation of the Ref
func (c *Ref) String() string {
	var tName, ntype string

	if c.IsNotNull {
		ntype = " NOT NULL"
	}
	if c.DisplayTableName {
		tName = c.TableName
	}

	ret := "{"
	if tName != "" {
		ret += tName + "."
	}
	ret += c.ColName + ", " + tokens.IDName(c.ColType) + ntype + "}"
	return ret
}

// DisplayName returns the display name of the Ref
func (c *Ref) DisplayName() string {
	ret := ""
	if c.DisplayTableName && c.TableName != "" {
		if c.TableAlias != "" {
			ret += c.TableAlias + "."
		} else {
			ret += c.TableName + "."
		}
	}
	ret += c.ColName
	return ret
}

// GetTableName returns the TableName alias or if that is empty,  the TableName  of the Ref
func (c *Ref) GetTableName() string {
	if c.TableAlias != "" {
		return c.TableAlias
	}
	return c.TableName
}

// MergeRefDef combines a Ref & a Def
// colA is the one that was used to find colB
// colB is the original Def of a table
func MergeRefDef(colA Ref, colB Def) (Ref, error) {
	var result Ref
	// ColName should be the same
	result.ColName = colA.ColName
	if colA.ColName != colB.ColName {
		return result, sqerr.NewInternalf("Can't merge Ref %s, %s", colA.ColName, colB.ColName)
	}
	result.ColType = colB.ColType
	result.Idx = colB.Idx
	result.IsNotNull = colB.IsNotNull

	result.TableName = colB.TableName
	if colA.TableName != colB.TableName {
		// Use colA TableName as alias
		result.TableAlias = colA.TableName
	} else {
		result.TableAlias = colA.TableAlias
	}

	//Display table Name of original is preserved
	result.DisplayTableName = colA.DisplayTableName
	return result, nil
}

//Encode outputs a binary encoded version of the Ref to the codec
func (c *Ref) Encode(enc *sqbin.Codec) {

	enc.WriteString(c.ColName)
	enc.WriteUint64(uint64(c.ColType))
	enc.WriteInt(c.Idx)
	enc.WriteBool(c.IsNotNull)
	enc.WriteString(c.TableName)
	enc.WriteString(c.TableAlias)
	enc.WriteBool(c.DisplayTableName)
}

//Decode a binary encoded version of a Ref from the codec
func (c *Ref) Decode(dec *sqbin.Codec) {

	c.ColName = dec.ReadString()
	c.ColType = tokens.TokenID(dec.ReadUint64())
	c.Idx = dec.ReadInt()
	c.IsNotNull = dec.ReadBool()
	c.TableName = dec.ReadString()
	c.TableAlias = dec.ReadString()
	c.DisplayTableName = dec.ReadBool()
}
