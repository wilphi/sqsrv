package sqtables

import (
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
	return ColDef{colName, colType, -1, isNotNull}
}

// ToString returns a string representation of the ColDef
func (c *ColDef) ToString() string {
	var ntype string
	if c.IsNotNull {
		ntype = ", NOT NULL"
	}
	return "{" + c.ColName + ", " + c.ColType + ntype + "}"
}

//Encode outputs a binary encoded version of the coldef to the codec
func (c *ColDef) Encode(enc *sqbin.Codec) {

	enc.WriteString(c.ColName)
	enc.WriteString(c.ColType)
	enc.WriteInt(c.Idx)
	enc.WriteBool(c.IsNotNull)
}

//Decode a binary encoded version of a coldef from the codec
func (c *ColDef) Decode(dec *sqbin.Codec) {

	c.ColName = dec.ReadString()
	c.ColType = dec.ReadString()
	c.Idx = dec.ReadInt()
	c.IsNotNull = dec.ReadBool()

}

//////////////////////////////////////////////////////////////////

// NewColListDefs - Create a list of columns based on ColDefs
func NewColListDefs(colD []ColDef) ColList {
	colNames := make([]string, len(colD))
	valid := true
	for i, col := range colD {
		colNames[i] = col.ColName
		if col.Idx == -1 {
			valid = false
		}
	}
	return ColList{colD: colD, defsValid: valid, colNames: colNames}
}

// NewColListNames - Create a list of columns based on name strings
func NewColListNames(colNames []string) ColList {
	colD := make([]ColDef, len(colNames))
	for i, name := range colNames {
		colD[i].ColName = name
	}
	return ColList{colD: colD, defsValid: false, colNames: colNames}
}

//ValidateTable -
func (cl *ColList) ValidateTable(profile *sqprofile.SQProfile, tab *TableDef) error {
	var cd *ColDef
	cl.isCount = false
	cl.isCols = false
	colDefs := make([]ColDef, len(cl.colNames))
	for i, name := range cl.colNames {
		if name == tokens.Count {
			ncd := CreateColDef(name, "FUNCTION", false)
			cd = &ncd
			cl.isCount = true
		} else {
			cd = tab.FindColDef(profile, name)
			if cd == nil {
				return sqerr.Newf("Table %s does not have a column named %s", tab.GetName(profile), name)
			}
			cl.isCols = true
		}

		colDefs[i] = *cd
	}
	if cl.isCount && cl.isCols {
		return sqerr.New("The function Count can not be used with Columns")
	}

	cl.defsValid = true
	cl.colD = colDefs
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
