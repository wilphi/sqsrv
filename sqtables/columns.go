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
	ColName          string
	ColType          string
	TableName        string
	Idx              int
	IsNotNull        bool
	DisplayTableName bool
}

// ColList - a list of column definitions
type ColList struct {
	colD      []ColDef
	defsValid bool
	colNames  []string
	isCols    bool
	isCount   bool
}

// NewColDef -
func NewColDef(colName string, colType string, isNotNull bool) ColDef {
	return ColDef{ColName: colName, ColType: colType, Idx: -1, IsNotNull: isNotNull, DisplayTableName: false}
}

// ToString returns a string representation of the ColDef
func (c *ColDef) ToString() string {
	var tName, ntype string

	if c.IsNotNull {
		ntype = " NOT NULL"
	}
	if c.DisplayTableName {
		tName = c.TableName
	}

	ret := "{" + Ternary(tName != "", tName+".", "") + c.ColName + ", " + c.ColType + ntype + "}"
	return ret
}

// DisplayName returns the display name of the ColDef
func (c *ColDef) DisplayName() string {

	return Ternary(c.DisplayTableName && c.TableName != "", c.TableName+".", "") + c.ColName
}

// MergeColDef combines two ColDef
// colA is the one that was used to find colB
// colB is likely to be the original ColDef of a table
func MergeColDef(colA, colB ColDef) (ColDef, error) {
	var result ColDef
	// ColName should be the same
	result.ColName = colA.ColName
	if colA.ColName != colB.ColName {
		return result, sqerr.NewInternalf("Can't merge ColDef %s, %s", colA.ColName, colB.ColName)
	}
	result.ColType = colB.ColType
	result.Idx = colB.Idx
	result.IsNotNull = colB.IsNotNull
	result.TableName = colB.TableName
	//Display table Name of original is preserved
	result.DisplayTableName = colA.DisplayTableName
	return result, nil
}

//Encode outputs a binary encoded version of the coldef to the codec
func (c *ColDef) Encode(enc *sqbin.Codec) {

	enc.WriteString(c.ColName)
	enc.WriteString(c.ColType)
	enc.WriteInt(c.Idx)
	enc.WriteBool(c.IsNotNull)
	enc.WriteString(c.TableName)
	enc.WriteBool(c.DisplayTableName)
}

//Decode a binary encoded version of a coldef from the codec
func (c *ColDef) Decode(dec *sqbin.Codec) {

	c.ColName = dec.ReadString()
	c.ColType = dec.ReadString()
	c.Idx = dec.ReadInt()
	c.IsNotNull = dec.ReadBool()
	c.TableName = dec.ReadString()
	c.DisplayTableName = dec.ReadBool()
}

//////////////////////////////////////////////////////////////////

// NewColListDefs - Create a list of columns based on ColDefs
func NewColListDefs(colD []ColDef) *ColList {
	colNames := make([]string, len(colD))
	valid := true
	for i, col := range colD {
		colNames[i] = col.ColName
		if col.Idx == -1 || col.TableName == "" {
			valid = false
		}
	}
	return &ColList{colD: colD, defsValid: valid, colNames: colNames}
}

// NewColListNames - Create a list of columns based on name strings
func NewColListNames(colNames []string) *ColList {
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
	return &ColList{colD: colD, defsValid: false, colNames: colNames}
}

//Validate - Makes sure that all cols in the list are valid columns in the given table list
func (cl *ColList) Validate(profile *sqprofile.SQProfile, tables *TableList) error {
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

// FindColDef finds a coldef in the list
func (cl *ColList) FindColDef(name string) *ColDef {
	for _, cd := range cl.colD {
		if cd.ColName == name {
			return &cd
		}
	}
	return nil
}
