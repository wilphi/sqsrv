package sqtables

import (
	"sort"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// DataSetTableName is a marker used to indicate that the "table" used to evalate data is from a dataset
// it is formatted in a way that can not be reproduced from a parsed identifier
const DataSetTableName = " _dataset"

// DataSetMoniker is the moniker for the DataSetTableName
var DataSetMoniker = moniker.New(DataSetTableName, "")

// DataSet - structure that contains a row/column set including column definitions
type DataSet struct {
	Vals       [][]sqtypes.Value
	usePtrs    bool
	Ptrs       sqptr.SQPtrs
	tables     *TableList
	order      []OrderItem
	validOrder bool
	eList      *ExprList
}

// OrderItem stores information for ORDER BY clause
type OrderItem struct {
	ColName  string
	SortType tokens.TokenID
	idx      int
}

// GetColNames - returns a string array of column names
func (d *DataSet) GetColNames() []string {

	return d.eList.Names(false)
}

//NewDataSet creates a dataset based on a list of expressions
func NewDataSet(profile *sqprofile.SQProfile, tables *TableList, eList *ExprList) (*DataSet, error) {
	var err error

	if eList == nil || eList.Len() == 0 {
		return nil, sqerr.NewInternal("Expression List is empty for new DataSet")
	}
	// Verify all cols exist in table list
	if err = eList.ValidateCols(profile, tables); err != nil {
		return nil, err
	}

	return &DataSet{eList: eList, tables: tables}, nil
}

// NumCols -
func (d *DataSet) NumCols() int {
	return d.eList.Len()
}

// GetColList -
func (d *DataSet) GetColList() *column.List {
	cols := make([]column.Ref, d.eList.Len())
	for i, ex := range d.eList.exprlist {
		cols[i] = ex.ColRef()
	}
	return column.NewListRefs(cols)
}

// GetTables -
func (d *DataSet) GetTables() *TableList {
	return d.tables
}

// SetOrder -
func (d *DataSet) SetOrder(order []OrderItem) error {
	d.validOrder = false
	d.order = order
	for x, col := range d.order {
		//set the index
		d.order[x].idx = d.eList.FindName(col.ColName)
		if d.order[x].idx < 0 {
			// Col not found
			return sqerr.Newf("Column %s not found in dataset", col.ColName)
		}
	}
	d.validOrder = true
	return nil
}

// Len - used for sorting
func (d *DataSet) Len() int {
	if d.Vals == nil {
		return 0
	}
	return len(d.Vals)

}

// Swap - used for sorting
func (d *DataSet) Swap(i, j int) {
	d.Vals[i], d.Vals[j] = d.Vals[j], d.Vals[i]
}

// Less is part of sort Interface
func (d *DataSet) Less(i, j int) bool {
	if len(d.order) > 0 {
		for x := range d.order {
			col := d.order[x]
			nullA := d.Vals[i][col.idx] == nil || d.Vals[i][col.idx].IsNull()
			nullB := d.Vals[j][col.idx] == nil || d.Vals[j][col.idx].IsNull()
			if nullA && nullB {
				continue
			}
			if d.Vals[i][col.idx].LessThan(d.Vals[j][col.idx]) || nullB {
				return col.SortType == tokens.Asc
			}
			if d.Vals[i][col.idx].GreaterThan(d.Vals[j][col.idx]) || nullA {
				return col.SortType != tokens.Asc
			}
		}
	} else {
		for x := 0; x < d.eList.Len(); x++ {
			nullA := d.Vals[i][x].IsNull()
			nullB := d.Vals[j][x].IsNull()
			if nullA && nullB {
				continue
			}
			if d.Vals[i][x].LessThan(d.Vals[j][x]) || nullB {
				return true
			}
			if d.Vals[i][x].GreaterThan(d.Vals[j][x]) || nullA {
				return false
			}
		}
	}
	return true
}

// Distinct sorts and removes duplicate rows in the data set
func (d *DataSet) Distinct() {
	sort.Sort(d)
	if (len(d.Vals) - 1) > 0 {
		tmp := d.Vals[:1]
		for i := 0; i < len(d.Vals)-1; i++ {
			match := false
			for j := 0; j < len(d.Vals[i]); j++ {
				if d.Vals[i][j].Equal(d.Vals[i+1][j]) {
					match = true
				} else {
					match = false
					break
				}
			}
			if !match {
				tmp = append(tmp, d.Vals[i+1])
			}
		}
		d.Vals = tmp
	}
}

// Sort is a convenience function
func (d *DataSet) Sort() error {
	if len(d.order) <= 0 || !d.validOrder {
		return sqerr.New("Sort Order has not been set for DataSet")
	}

	sort.Sort(d)
	return nil
}

// DSRow defines row definition for datasets
type DSRow struct {
	Ptr       sqptr.SQPtr
	Vals      []sqtypes.Value
	TableName string
}

// GetTableName gets the table name that the DSRow is based off on.
func (r *DSRow) GetTableName(profile *sqprofile.SQProfile) string {
	return DataSetTableName
}

// GetPtr returns the pointer to the given row
func (r *DSRow) GetPtr(profile *sqprofile.SQProfile) sqptr.SQPtr {
	return r.Ptr
}

// ColVal -
func (r *DSRow) ColVal(profile *sqprofile.SQProfile, c *column.Ref) (sqtypes.Value, error) {
	if c.Idx < 0 || c.Idx >= len(r.Vals) {
		return nil, sqerr.Newf("Invalid index (%d) for Column in row. Col len = %d", c.Idx, len(r.Vals))
	}
	return r.Vals[c.Idx], nil
}

// IdxVal gets the value of the col at the index idx
func (r *DSRow) IdxVal(profile *sqprofile.SQProfile, idx int) (sqtypes.Value, error) {
	if idx < 0 || idx >= len(r.Vals) {
		return nil, sqerr.Newf("Invalid index (%d) for row. Data len = %d", idx, len(r.Vals))
	}
	return r.Vals[idx], nil
}
