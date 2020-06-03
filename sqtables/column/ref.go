package column

import (
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/tokens"
)

// Ref - reference to a column definition
type Ref struct {
	ColName          string
	ColType          tokens.TokenID
	Idx              int
	IsNotNull        bool
	TableName        *moniker.Moniker
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
		tName = c.TableName.Show()
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
	if c.DisplayTableName && c.TableName != nil {
		ret += c.TableName.Show() + "."
	}
	ret += c.ColName
	return ret
}

// GetTableName returns the TableName alias or if that is empty,  the TableName  of the Ref
func (c *Ref) GetTableName() string {
	if c.TableName == nil {
		return ""
	}
	return c.TableName.Show()
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

	if colA.TableName != nil {
		if colA.TableName.Name != colB.TableName {
			// Use colA TableName as alias
			result.TableName = moniker.New(colB.TableName, colA.TableName.Name)
		} else {
			result.TableName = moniker.New(colB.TableName, colA.TableName.Alias)
		}
	} else {
		result.TableName = moniker.New(colB.TableName, "")
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
	if c.TableName != nil {
		enc.WriteString(c.TableName.Name)
		enc.WriteString(c.TableName.Alias)
	} else {
		enc.WriteString("")
		enc.WriteString("")
	}

	enc.WriteBool(c.DisplayTableName)
}

//Decode a binary encoded version of a Ref from the codec
func (c *Ref) Decode(dec *sqbin.Codec) {

	c.ColName = dec.ReadString()
	c.ColType = tokens.TokenID(dec.ReadUint64())
	c.Idx = dec.ReadInt()
	c.IsNotNull = dec.ReadBool()
	c.TableName = moniker.New(dec.ReadString(), dec.ReadString())
	if c.TableName.Name == "" {
		c.TableName = nil
	}
	c.DisplayTableName = dec.ReadBool()
}
