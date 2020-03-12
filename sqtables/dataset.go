package sqtables

import (
	"sort"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// DataSet - structure that contains a row/column set including column definitions
type DataSet struct {
	//cols       ColList
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
	SortType string
	idx      int
}

// GetColNames - returns a string array of column names
func (d *DataSet) GetColNames() []string {

	return d.eList.GetNames()
}

// NewDataSet -
func NewDataSet(profile *sqprofile.SQProfile, tables *TableList, cols ColList) (*DataSet, error) {
	if cols.Len() == 0 {
		return nil, nil
	}
	err := cols.ValidateTable(profile, tables)
	if err != nil {
		return nil, err
	}
	eList := ColsToExpr(cols)
	return &DataSet{eList: eList, tables: tables}, nil
}

// NewExprDataSet creates a dataset based on expressions
func NewExprDataSet(tables *TableList, eList *ExprList) *DataSet {
	if eList == nil || eList.Len() == 0 {
		return nil //&DataSet{tables: tables}
	}
	return &DataSet{eList: eList, tables: tables}
}

// NumCols -
func (d *DataSet) NumCols() int {
	return d.eList.Len()
}

// GetColList -
func (d *DataSet) GetColList() ColList {
	cols := make([]ColDef, d.eList.Len())
	for i, ex := range d.eList.exprlist {
		cols[i] = ex.ColDef()
	}
	return NewColListDefs(cols)
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
			if d.Vals[i][col.idx].LessThan(d.Vals[j][col.idx]) {
				return col.SortType == tokens.Asc
			}
			if d.Vals[i][col.idx].GreaterThan(d.Vals[j][col.idx]) {
				return col.SortType != tokens.Asc
			}
		}
	} else {
		for x := 0; x < d.eList.Len(); x++ {
			if d.Vals[i][x].LessThan(d.Vals[j][x]) {
				return true
			}
			if d.Vals[i][x].GreaterThan(d.Vals[j][x]) {
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
