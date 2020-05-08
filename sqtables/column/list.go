package column

import (
	"strings"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/tokens"
)

// List - a list of column references
type List struct {
	cols      []Ref
	defsValid bool
	colNames  []string
	isCols    bool
	isCount   bool
}

// ITableList interface for tablelists
type ITableList interface {
	FindDef(profile *sqprofile.SQProfile, colName, tableName string) (*Def, error)
}

//////////////////////////////////////////////////////////////////

// NewListDefs - Create a list of columns based on Defs
func NewListDefs(colD []Def) *List {
	colNames := make([]string, len(colD))
	cols := make([]Ref, len(colD))
	valid := true
	for i, col := range colD {
		colNames[i] = col.ColName
		if col.Idx == -1 || col.TableName == "" {
			valid = false
		}
		cols[i] = col.Ref()
	}
	return &List{cols: cols, defsValid: valid, colNames: colNames}
}

// NewListRefs - Create a list of columns based on Refs
func NewListRefs(cols []Ref) *List {
	colNames := make([]string, len(cols))
	valid := true
	for i, col := range cols {
		colNames[i] = col.ColName
		if col.Idx == -1 || col.TableName == "" {
			valid = false
		}
	}
	return &List{cols: cols, defsValid: valid, colNames: colNames}
}

// NewListNames - Create a list of columns based on name strings
func NewListNames(colNames []string) *List {
	cols := make([]Ref, len(colNames))
	for i, name := range colNames {
		x := strings.Index(name, ".")
		if x != -1 && x < len(name) {
			cols[i].ColName = name[x+1:]
			cols[i].TableName = name[:x]
		} else {
			cols[i].ColName = name
		}
	}
	return &List{cols: cols, defsValid: false, colNames: colNames}
}

//Validate - Makes sure that all cols in the list are valid columns in the given table list
func (cl *List) Validate(profile *sqprofile.SQProfile, tables ITableList) error {
	if cl.defsValid {
		return nil
	}
	cl.isCount = false
	cl.isCols = false
	for i, cd := range cl.cols {
		if cd.ColName == tokens.IDName(tokens.Count) {
			cl.cols[i].ColType = tokens.Count
			cl.isCount = true
		} else {
			col, err := tables.FindDef(profile, cd.ColName, cd.GetTableName())
			if err != nil {
				return err
			}
			cl.cols[i] = col.Ref()
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
func (cl *List) GetColNames() []string {
	return cl.colNames
}

// GetRefs -
func (cl *List) GetRefs() []Ref {
	return cl.cols
}

// Len - get the number of columns in list
func (cl *List) Len() int {
	return len(cl.colNames)
}

// FindRef finds a Ref in the list
func (cl *List) FindRef(name string) *Ref {
	for _, cd := range cl.cols {
		if cd.ColName == name {
			return &cd
		}
	}
	return nil
}
