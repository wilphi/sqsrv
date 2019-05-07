package sqtables

import (
	"github.com/wilphi/sqsrv/sqtypes"
)

// DataSet - structure that contains a row/column set including column definitions
type DataSet struct {
	cols    ColList
	Vals    [][]sqtypes.Value
	usePtrs bool
	Ptrs    []int64
	table   *TableDef
}

// GetColNames - returns a string array of column names
func (d *DataSet) GetColNames() []string {
	return d.cols.GetColNames()
}

// NewDataSet -
func NewDataSet(tab *TableDef, cols ColList) DataSet {
	return DataSet{cols: cols, table: tab}
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
