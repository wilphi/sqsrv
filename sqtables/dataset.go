package sqtables

import (
	"fmt"
	"sort"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// DataSet - structure that contains a row/column set including column definitions
type DataSet struct {
	cols       ColList
	Vals       [][]sqtypes.Value
	usePtrs    bool
	Ptrs       []int64
	table      *TableDef
	order      []OrderItem
	validOrder bool
}

// OrderItem stores information for ORDER BY clause
type OrderItem struct {
	ColName  string
	SortType string
	idx      int
}

// GetColNames - returns a string array of column names
func (d *DataSet) GetColNames() []string {
	return d.cols.GetColNames()
}

// NewDataSet -
func NewDataSet(tab *TableDef, cols ColList) *DataSet {
	if cols.Len() == 0 {
		return nil
	}
	return &DataSet{cols: cols, table: tab}
}

// NumCols -
func (d *DataSet) NumCols() int {
	return d.cols.Len()
}

// NumRows -
func (d *DataSet) NumRows() int {
	if d.Vals == nil {
		return 0
	}
	return len(d.Vals)
}

// GetColList -
func (d *DataSet) GetColList() ColList {
	return d.cols
}

// GetTable -
func (d *DataSet) GetTable() *TableDef {
	return d.table
}

// SetOrder -
func (d *DataSet) SetOrder(order []OrderItem) error {
	d.validOrder = false
	d.order = order
	for x, col := range d.order {
		//set the index
		d.order[x].idx = d.cols.FindColIdx(col.ColName)
		if d.order[x].idx < 0 {
			// Col not found
			return sqerr.New(fmt.Sprintf("Column %s not found in dataset", col.ColName))
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
	for x := range d.order {
		col := d.order[x]
		if d.Vals[i][col.idx].LessThan(d.Vals[j][col.idx]) {
			return col.SortType == tokens.Asc
		}
		if d.Vals[i][col.idx].GreaterThan(d.Vals[j][col.idx]) {
			return col.SortType != tokens.Asc
		}
	}
	return true
}

// Sort is a convenience function
func (d *DataSet) Sort() error {
	if len(d.order) <= 0 || !d.validOrder {
		return sqerr.New("Sort Order has not been set for DataSet")
	}

	sort.Sort(d)
	return nil
}
