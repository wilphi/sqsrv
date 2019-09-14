package sqtables

import (
	"strings"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/tokens"
)

// ColDef - column definition
type ColDef struct {
	ColName   string
	ColType   string
	Idx       int
	IsNotNull bool
	TableName string
}

// ColList - a list of column definitions
type ColList struct {
	colD      []ColDef
	defsValid bool
	colNames  []string
	isCols    bool
	isCount   bool
}

// CreateColDef -
func CreateColDef(colName string, colType string, isNotNull bool) ColDef {
	return ColDef{ColName: colName, ColType: colType, Idx: -1, IsNotNull: isNotNull}
}

// ToString returns a string representation of the ColDef
func (c *ColDef) ToString() string {
	var tName, ntype string

	if c.IsNotNull {
		ntype = " NOT NULL"
	}
	tName = c.TableName
	if tName != "" {
		tName += "."
	}
	return "{" + tName + c.ColName + ", " + c.ColType + ntype + "}"
}

//Encode outputs a binary encoded version of the coldef to the codec
func (c *ColDef) Encode(enc *sqbin.Codec) {

	enc.WriteString(c.ColName)
	enc.WriteString(c.ColType)
	enc.WriteInt(c.Idx)
	enc.WriteBool(c.IsNotNull)
	enc.WriteString(c.TableName)
}

//Decode a binary encoded version of a coldef from the codec
func (c *ColDef) Decode(dec *sqbin.Codec) {

	c.ColName = dec.ReadString()
	c.ColType = dec.ReadString()
	c.Idx = dec.ReadInt()
	c.IsNotNull = dec.ReadBool()
	c.TableName = dec.ReadString()
}

//////////////////////////////////////////////////////////////////

// NewColListDefs - Create a list of columns based on ColDefs
func NewColListDefs(colD []ColDef) ColList {
	colNames := make([]string, len(colD))
	valid := true
	for i, col := range colD {
		colNames[i] = col.ColName
		if col.Idx == -1 || col.TableName == "" {
			valid = false
		}
	}
	return ColList{colD: colD, defsValid: valid, colNames: colNames}
}

// NewColListNames - Create a list of columns based on name strings
func NewColListNames(colNames []string) ColList {
	colD := make([]ColDef, len(colNames))
	for i, name := range colNames {
		x := strings.Index(name, ".")
		if x != -1 && x < len(name) {
			colD[i].ColName = name[x+1:]
			colD[i].TableName = name[:x]
		} else {
			colD[i].ColName = name
		}
	}
	return ColList{colD: colD, defsValid: false, colNames: colNames}
}

//ValidateTable -
func (cl *ColList) ValidateTable(profile *sqprofile.SQProfile, tables *TableList) error {
	if cl.defsValid {
		return nil
	}
	cl.isCount = false
	cl.isCols = false
	for i, cd := range cl.colD {
		if cd.ColName == tokens.Count {
			cl.colD[i].ColType = "FUNCTION"
			cl.isCount = true
		} else {
			col, err := tables.FindColDef(profile, cd.ColName, cd.TableName)
			if err != nil {
				return err
			}
			cl.colD[i].ColType = col.ColType
			cl.colD[i].Idx = col.Idx
			cl.colD[i].IsNotNull = col.IsNotNull
			cl.isCols = true
		}

	}
	if cl.isCount && cl.isCols {
		return sqerr.New("The function Count can not be used with Columns")
	}

	cl.defsValid = true
	return nil
}

// GetColNames -
func (cl *ColList) GetColNames() []string {
	return cl.colNames
}

// GetColDefs -
func (cl *ColList) GetColDefs() []ColDef {
	return cl.colD
}

// Len - get the number of columns in list
func (cl *ColList) Len() int {
	return len(cl.colNames)
}
