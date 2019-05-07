package sqtables

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
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
	sqmutex.SQMutex
}

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

//var _rowID = new(int64)

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
//func (t *TableDef) AddRows(cols []string, vals [][]sqtypes.Value) (int, error) {
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

	includeRow := (conditions == nil)
	t.Lock(profile)
	defer t.Unlock(profile)

	// Get the pointers
	for i := range t.rowm {

		if conditions != nil {
			includeRow, err = conditions.Evaluate(profile, t.rowm[i])
			if err != nil {
				return []int64{}, err
			}
		}
		if !includeRow {
			continue
		}

		ptrs = append(ptrs, i)
	}

	// If no errors then delete

	return ptrs, DeleteRowsFromPtrs(profile, t, ptrs, SoftDelete)
}

//DeleteRowsFromPtrs deletes rows from a table based on the given list of pointers
func DeleteRowsFromPtrs(profile *sqprofile.SQProfile, t *TableDef, ptrs []int64, soft bool) error {
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

// GetRowData - Returns a dataset with the data from table
func (t *TableDef) GetRowData(profile *sqprofile.SQProfile, cols ColList, conditions Condition) (*DataSet, error) {
	var err error
	var nrows int
	t.RLock(profile)
	defer t.RUnlock(profile)
	// Verify all cols exist in table
	if err := cols.ValidateTable(profile, t); err != nil {
		return nil, err
	}

	ret := NewDataSet(t, cols)
	// num rows
	if cols.isCount {
		nrows = 1
		ret.usePtrs = false
	} else {
		nrows = len(t.rowm)
		ret.usePtrs = true
	}

	cnt := 0

	ncols := cols.Len()
	includeRow := true

	ret.Vals = make([][]sqtypes.Value, nrows)
	if ret.usePtrs {
		ret.Ptrs = make([]int64, nrows)
	}

	includeRow = (conditions == nil)

	for i, row := range t.rowm {
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
		if ret.usePtrs {
			if cnt > len(ret.Ptrs) {
				log.Panic("count is larger than array")
			}

			ret.Ptrs[cnt] = i
		}
		if row.RowID != i {
			panic("rowID does not match Map index")
		}

		if !cols.isCount {
			ret.Vals[cnt] = make([]sqtypes.Value, ncols)
			for j, col := range cols.GetColDefs() {
				ret.Vals[cnt][j] = row.Data[col.Idx]
			}
		}
		cnt++

	}
	if !cols.isCount {
		ret.Vals = ret.Vals[:cnt]
		ret.Ptrs = ret.Ptrs[:cnt]
	} else {
		ret.Vals[0] = make([]sqtypes.Value, ncols)
		ret.Vals[0][0] = sqtypes.NewSQInt(cnt)
	}
	return &ret, nil

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

// GetRowPtrs returns the full list of rowIDs for the table. It can be sorted or not.
func (t *TableDef) GetRowPtrs(profile *sqprofile.SQProfile, sorted bool) []int {

	// get the ordered list of RowIDs
	list := make([]int, len(t.rowm))

	i := 0
	for rowid := range t.rowm {
		list[i] = int(rowid)
		i++
	}
	if sorted {
		sort.Ints(list)
	}
	return list
}

// isUnderScore - Checks to see if first char in string is an underscore
func isUnderScore(name string) bool {
	for _, c := range name {
		return c == '_'
	}
	return false
}
