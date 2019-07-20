package sqtables

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqmutex"
	"github.com/wilphi/sqsrv/sqprofile"
	sqtypes "github.com/wilphi/sqsrv/sqtypes"
)

// Type of delete to be carried out by delete functions
const (
	SoftDelete = true
	HardDelete = false
)

// TableDef -  table definition
type TableDef struct {
	tableName  string
	tableCols  []ColDef
	rowm       map[int64]*RowDef
	nextOffset int64
	nextRowID  *int64
	isDropped  bool
	sqmutex.SQMutex
}

// RowOrder sets the default sort for a dataset. If true then all rows are sorted by the RowID
//   if false then the order will be random.
//   the Sort function on a data set will override the default sort
var RowOrder = false

// CreateTableDef -
func CreateTableDef(name string, cols ...ColDef) *TableDef {
	var tab TableDef

	tab.tableName = strings.ToLower(name)
	tab.tableCols = cols

	tab.SQMutex = sqmutex.NewSQMutex("Table: " + tab.tableName)

	log.Debugln("TableName: ", tab.tableName)
	for i, col := range cols {
		col.Idx = i
		tab.tableCols[i].Idx = i
	}

	log.Debugln("Cols: ", tab.tableCols)
	tab.rowm = make(map[int64]*RowDef)
	tab.nextOffset = 0
	tab.nextRowID = new(int64)
	return &tab
}

// GetName - Name of the table
func (t *TableDef) GetName(profile *sqprofile.SQProfile) string {
	t.RLock(profile)
	defer t.RUnlock(profile)
	return t.tableName
}

// RowCount -
func (t *TableDef) RowCount(profile *sqprofile.SQProfile) int {
	cnt := 0
	t.RLock(profile)
	defer t.RUnlock(profile)
	if t.rowm != nil {

		for rowid := range t.rowm {
			if !t.rowm[rowid].isDeleted {
				cnt++
			}

		}
	}
	return cnt
}

// ToString - Thread safe version of ToString for a tabledef
func (t *TableDef) ToString(profile *sqprofile.SQProfile) string {
	t.RLock(profile)
	defer t.RUnlock(profile)
	cs := ""
	tabName := t.tableName
	for _, col := range t.tableCols {
		cs += col.toString()
	}
	return fmt.Sprintf("{%s Cols:%s}\n", tabName, cs)

}

// AddRows - add one or more rows to table
func (t *TableDef) AddRows(profile *sqprofile.SQProfile, data *DataSet) (int, error) {

	// Create all of the rows before locking and adding them to the table
	newRows := make([]*RowDef, data.NumRows())
	data.Ptrs = make([]int64, data.NumRows())

	for cnt, val := range data.Vals {
		rowID := atomic.AddInt64(t.nextRowID, 1)
		row, err := CreateRow(profile, rowID, t, data.GetColNames(), val)
		if err != nil {
			return 0, err
		}
		newRows[cnt] = row
	}

	t.Lock(profile)
	for i, r := range newRows {
		t.rowm[r.RowID] = r
		data.Ptrs[i] = r.RowID
	}
	t.Unlock(profile)

	return len(newRows), nil
}

// GetTable - Get a pointer to the named table if it exists
func GetTable(profile *sqprofile.SQProfile, name string) *TableDef {
	return _tables.FindTableDef(profile, name)
}

// DeleteRows - Delete rows based on where condition
func (t *TableDef) DeleteRows(profile *sqprofile.SQProfile, conditions Condition) ([]int64, error) {
	var err error
	var ptrs []int64

	t.Lock(profile)
	defer t.Unlock(profile)

	ptrs, err = t.GetRowPtrs(profile, conditions, false)

	// If no errors then delete
	if err != nil {
		return nil, err
	}

	return ptrs, t.DeleteRowsFromPtrs(profile, ptrs, SoftDelete)
}

//DeleteRowsFromPtrs deletes rows from a table based on the given list of pointers
func (t *TableDef) DeleteRowsFromPtrs(profile *sqprofile.SQProfile, ptrs []int64, soft bool) error {
	t.Lock(profile)
	defer t.Unlock(profile)
	for _, idx := range ptrs {
		if soft == SoftDelete {
			t.rowm[idx].isDeleted = true
			t.rowm[idx].isModified = true
		} else {
			delete(t.rowm, idx)
		}
	}
	return nil
}

// GetRowDataFromPtrs returns data based on the rowIDs passed
func (t *TableDef) GetRowDataFromPtrs(profile *sqprofile.SQProfile, ptrs []int64) (*DataSet, error) {
	ds, err := NewDataSet(profile, t, t.GetCols(profile))
	if err != nil {
		return nil, err
	}
	ds.Vals = make([][]sqtypes.Value, len(ptrs))
	t.RLock(profile)
	defer t.RUnlock(profile)
	for i, idx := range ptrs {
		row, ok := t.rowm[idx]
		if !ok {
			return nil, sqerr.New(fmt.Sprintf("Row %d does not exist", idx))
		}
		ds.Vals[i] = append(row.Data[:0:0], row.Data...)
	}
	return ds, nil
}

// GetRowData - Returns a dataset with the data from table
func (t *TableDef) GetRowData(profile *sqprofile.SQProfile, eList *ExprList, conditions Condition) (*DataSet, error) {
	var err error

	t.RLock(profile)
	defer t.RUnlock(profile)

	// Verify all cols exist in table
	if err = eList.ValidateCols(profile, t); err != nil {
		return nil, err
	}

	// Setup the dataset for the results
	ret := NewExprDataSet(t, eList)
	ret.usePtrs = !eList.HasCount()

	// Get the pointers to the rows based on the conditions
	ptrs, err := t.GetRowPtrs(profile, conditions, RowOrder)
	if err != nil {
		return nil, err
	}

	if eList.HasCount() {
		ret.Vals = make([][]sqtypes.Value, 1)
		ret.Vals[0] = make([]sqtypes.Value, eList.Len())
		ret.Vals[0][0] = sqtypes.NewSQInt(len(ptrs))
	} else {
		ret.Vals = make([][]sqtypes.Value, len(ptrs))
		ret.Ptrs = ptrs

		for i, ptr := range ptrs {
			// make sure the ptr points to the correct row
			if t.rowm[ptr].RowID != ptr {
				log.Panic("rowID does not match Map index")
			}

			ret.Vals[i], err = eList.Evaluate(profile, t.rowm[ptr])
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, nil

}

// FindCol - Returns the col index and col Type index < 0 if not found
func (t *TableDef) FindCol(profile *sqprofile.SQProfile, name string) (int, string) {
	var i int
	var col ColDef
	t.RLock(profile)
	defer t.RUnlock(profile)
	for i, col = range t.tableCols {
		if col.ColName == name {
			return i, col.ColType
		}
	}
	return -1, ""
}

// FindColDef - Returns coldef based on name
func (t *TableDef) FindColDef(profile *sqprofile.SQProfile, name string) *ColDef {
	var col ColDef
	t.RLock(profile)
	defer t.RUnlock(profile)

	for _, col = range t.tableCols {
		if col.ColName == name {
			return &col
		}
	}
	return nil
}

// GetCols - Returns the list of col TypeDef for the table
func (t *TableDef) GetCols(profile *sqprofile.SQProfile) ColList {
	t.RLock(profile)
	defer t.RUnlock(profile)
	return NewColListDefs(t.tableCols)
}

// GetColNames - returns cols names for the table
func (t *TableDef) GetColNames(profile *sqprofile.SQProfile) []string {
	cols := make([]string, len(t.tableCols))
	t.RLock(profile)
	defer t.RUnlock(profile)

	for i, col := range t.tableCols {
		cols[i] = col.ColName
	}
	return cols
}

// NumCol - The number of columns in the table
func (t *TableDef) NumCol(profile *sqprofile.SQProfile) int {
	t.RLock(profile)
	defer t.RUnlock(profile)
	return len(t.tableCols)
}

// GetRowPtrs returns the list of rowIDs for the table based on the conditions.
//    If the conditions are nil, then all rows are returned. The list can be sorted or not.
//    By default the table is Read Locked, to have a write lock the calling function must do it.
func (t *TableDef) GetRowPtrs(profile *sqprofile.SQProfile, conditions Condition, sorted bool) ([]int64, error) {
	var err error
	var ptrs []int64

	includeRow := (conditions == nil)
	t.RLock(profile)
	defer t.RUnlock(profile)

	for rowID, row := range t.rowm {
		if row == nil || row.isDeleted {
			continue
		}
		if conditions != nil {
			includeRow, err = conditions.Evaluate(profile, row)
			if err != nil {
				return nil, err
			}
		}
		if !includeRow {
			continue
		}

		ptrs = append(ptrs, rowID)
	}
	if sorted {
		sort.Slice(ptrs, func(i, j int) bool { return ptrs[i] < ptrs[j] })
	}
	return ptrs, nil
}

//UpdateRowsFromPtrs updates rows in the table based on the given list of pointers, columns to be changed and values to be set
func (t *TableDef) UpdateRowsFromPtrs(profile *sqprofile.SQProfile, ptrs []int64, cols []string, eList *ExprList) error {
	t.Lock(profile)
	defer t.Unlock(profile)
	for _, idx := range ptrs {
		row, ok := t.rowm[idx]
		if row == nil || !ok {
			return sqerr.NewInternal(fmt.Sprintf("Row %d does not exist for update", idx))
		}
		vals, err := eList.Evaluate(profile, row)
		if err != nil {
			return err
		}
		err = row.UpdateRow(profile, cols, vals)
		if err != nil {
			return err
		}

	}
	return nil
}

// GetRow -
func (t *TableDef) GetRow(profile *sqprofile.SQProfile, RowID int64) *RowDef {
	row, ok := t.rowm[RowID]
	if !ok || row == nil || row.isDeleted {
		return nil
	}
	return row
}

// isUnderScore - Checks to see if first char in string is an underscore
func isUnderScore(name string) bool {
	for _, c := range name {
		return c == '_'
	}
	return false
}
