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
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// Type of delete to be carried out by delete functions
const (
	SoftDelete = true
	HardDelete = false
)

// TableDef -  table definition
type TableDef struct {
	tableName   string       // immutable
	tableCols   []column.Def // immutable
	rowm        map[sqptr.SQPtr]*RowDef
	constraints []Constraint
	nextOffset  int64
	nextRowID   *uint64
	isDropped   bool
	*sqmutex.SQMtx
}

// TableRef structure holds the alias name, actual tableName and pointer to actual table
type TableRef struct {
	//TableName string
	//Alias     string
	Name  *moniker.Moniker
	Table *TableDef
}

// RowOrder sets the default sort for a dataset. If true then all rows are sorted by the RowID
//   if false then the order will be the default golang map order (somewhat random).
//   the Sort function on a data set will override the default sort
var RowOrder = false

// CreateTableDef -
func CreateTableDef(name string, cols []column.Def) *TableDef {
	var tab TableDef

	tab.tableName = strings.ToLower(name)
	tab.tableCols = cols

	tab.SQMtx = sqmutex.NewSQMtx("Table: " + tab.tableName)

	log.Debugln("TableName: ", tab.tableName)
	for i := range tab.tableCols {
		// set the order for the cols based on the order passed in
		tab.tableCols[i].Idx = i
		// set the tablename for the cols
		tab.tableCols[i].TableName = tab.tableName
	}

	log.Debugln("Cols: ", tab.tableCols)
	tab.rowm = make(map[sqptr.SQPtr]*RowDef)
	tab.nextOffset = 0
	tab.nextRowID = new(uint64)
	return &tab
}

// AddConstraints -
func (t *TableDef) AddConstraints(profile *sqprofile.SQProfile, constraints []Constraint) error {
	var err error
	t.Lock(profile)
	defer t.Unlock(profile)

	if t.constraints == nil {
		t.constraints = constraints
	} else {
		t.constraints = append(t.constraints, constraints...)
	}

	// Sort the constraints so that the order is PK, FK, Unique, Index
	sort.SliceStable(t.constraints, func(i int, j int) bool { return t.constraints[i].Ordering() < t.constraints[j].Ordering() })

	for i := range t.constraints {
		err = t.constraints[i].Validate(profile, t)
		if err != nil {
			return err
		}

	}
	return nil
}

// GetName - Name of the table
func (t *TableDef) GetName(profile *sqprofile.SQProfile) string {
	return t.tableName
}

// RowCount -
func (t *TableDef) RowCount(profile *sqprofile.SQProfile) (int, error) {
	cnt := 0
	err := t.RLock(profile)
	if err != nil {
		return -1, err
	}
	defer t.RUnlock(profile)
	if t.rowm != nil {

		for rowid := range t.rowm {
			if !t.rowm[rowid].isDeleted {
				cnt++
			}

		}
	}
	return cnt, nil
}

// TableRef returns a table reference to the table def
func (t *TableDef) TableRef(profile *sqprofile.SQProfile) *TableRef {
	return &TableRef{Name: moniker.New(t.tableName, ""), Table: t}
}

// String -
func (t *TableDef) String(profile *sqprofile.SQProfile) string {
	sLines := "--------------------------------------\n"
	cs := t.tableName + "\n" + sLines

	for _, col := range t.tableCols {
		cs += fmt.Sprintf("\t%s\n", col.String())
	}

	if len(t.constraints) > 0 {
		cs += sLines
		for _, con := range t.constraints {
			cs += fmt.Sprintf("\t%s\n", con.String())
		}
	}
	return cs

}

// AddRows - add one or more rows to table
func (t *TableDef) AddRows(profile *sqprofile.SQProfile, data *DataSet) (int, error) {

	// Create all of the rows before locking and adding them to the table
	newRows := make([]*RowDef, data.Len())
	data.Ptrs = make(sqptr.SQPtrs, data.Len())

	for cnt, val := range data.Vals {
		rowID := atomic.AddUint64(t.nextRowID, 1)
		row, err := CreateRow(profile, sqptr.SQPtr(rowID), t, data.GetColNames(), val)
		if err != nil {
			return -1, err
		}
		newRows[cnt] = row
	}

	err := t.Lock(profile)
	defer t.Unlock(profile)
	if err != nil {
		return -1, err
	}
	for i, r := range newRows {
		t.rowm[r.RowPtr] = r
		data.Ptrs[i] = r.RowPtr
	}

	return len(newRows), nil
}

// DeleteRows - Delete rows based on where expression
func (t *TableDef) DeleteRows(profile *sqprofile.SQProfile, whereExpr Expr) (ptrs sqptr.SQPtrs, err error) {

	err = t.Lock(profile)
	if err != nil {
		return
	}
	defer t.Unlock(profile)

	ptrs, err = t.GetRowPtrs(profile, whereExpr, false)

	// If no errors then delete
	if err != nil {
		return
	}

	return ptrs, t.DeleteRowsFromPtrs(profile, ptrs, SoftDelete)
}

//DeleteRowsFromPtrs deletes rows from a table based on the given list of pointers
func (t *TableDef) DeleteRowsFromPtrs(profile *sqprofile.SQProfile, ptrs sqptr.SQPtrs, soft bool) error {
	err := t.Lock(profile)
	if err != nil {
		return err
	}
	defer t.Unlock(profile)
	for _, idx := range ptrs {
		if soft == SoftDelete {
			t.rowm[idx].Delete(profile)
		} else {
			delete(t.rowm, idx)
		}
	}
	return nil
}

// GetRowDataFromPtrs returns data based on the rowIDs passed
func (t *TableDef) GetRowDataFromPtrs(profile *sqprofile.SQProfile, ptrs sqptr.SQPtrs) (*DataSet, error) {
	tables := NewTableListFromTableDef(profile, t)
	ds, err := NewDataSet(profile, tables, ColsToExpr(t.GetCols(profile)))
	if err != nil {
		return nil, err
	}
	ds.Vals = make([][]sqtypes.Value, len(ptrs))
	err = t.RLock(profile)
	if err != nil {
		return nil, err
	}
	defer t.RUnlock(profile)
	for i, idx := range ptrs {
		row, ok := t.rowm[idx]
		if !ok {
			return nil, sqerr.Newf("Row %d does not exist", idx)
		}
		ds.Vals[i] = append(row.Data[:0:0], row.Data...)
	}
	return ds, nil
}

// FindCol - Returns the col index and col Type index < 0 if not found
func (t *TableDef) FindCol(profile *sqprofile.SQProfile, name string) (int, tokens.TokenID) {
	var i int
	var col column.Def
	for i, col = range t.tableCols {
		if col.ColName == name {
			return i, col.ColType
		}
	}
	return -1, tokens.NilToken
}

// FindColDef - Returns column.Def based on name
func (t *TableDef) FindColDef(profile *sqprofile.SQProfile, name string) *column.Def {

	for _, col := range t.tableCols {
		if col.ColName == name {
			return &col
		}
	}
	return nil
}

// GetCols - Returns the list of col TypeDef for the table
func (t *TableDef) GetCols(profile *sqprofile.SQProfile) *column.List {
	return column.NewListDefs(t.tableCols)
}

// GetColNames - returns cols names for the table
func (t *TableDef) GetColNames(profile *sqprofile.SQProfile) []string {
	cols := make([]string, len(t.tableCols))

	for i, col := range t.tableCols {
		cols[i] = col.ColName
	}
	return cols
}

// NumCol - The number of columns in the table
func (t *TableDef) NumCol(profile *sqprofile.SQProfile) int {
	return len(t.tableCols)
}

// GetRowPtrs returns the list of rowIDs for the table based on the expression.
//    If the expression is nil, then all rows are returned. The list can be sorted or not.
//    By default the table is Read Locked, to have a write lock the calling function must do it.
func (t *TableDef) GetRowPtrs(profile *sqprofile.SQProfile, exp Expr, sorted bool) (ptrs sqptr.SQPtrs, err error) {
	var val sqtypes.Value

	includeRow := (exp == nil)
	err = t.RLock(profile)
	if err != nil {
		return nil, err
	}

	defer t.RUnlock(profile)

	for rowID, row := range t.rowm {
		if row == nil || row.isDeleted {
			continue
		}
		if exp != nil {
			val, err = exp.Evaluate(profile, EvalPartial, row)
			if err != nil {
				return nil, err
			}
			if val != nil {
				boolVal, ok := val.(sqtypes.SQBool)
				if ok {
					includeRow = boolVal.Bool()
				}
			} else {
				includeRow = true
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
func (t *TableDef) UpdateRowsFromPtrs(profile *sqprofile.SQProfile, ptrs sqptr.SQPtrs, cols []string, eList *ExprList) error {
	err := eList.ValidateCols(profile, NewTableListFromTableDef(profile, t))
	if err != nil {
		return err
	}

	err = t.Lock(profile)
	if err != nil {
		return err
	}
	defer t.Unlock(profile)
	for _, idx := range ptrs {
		row, ok := t.rowm[idx]
		if row == nil || !ok {
			return sqerr.NewInternalf("Row %d does not exist for update", idx)
		}
		vals, err := eList.Evaluate(profile, EvalFull, row)
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
func (t *TableDef) GetRow(profile *sqprofile.SQProfile, RowPtr sqptr.SQPtr) *RowDef {
	row, ok := t.rowm[RowPtr]
	if !ok || row == nil || row.isDeleted {
		return nil
	}
	return row
}

// GetRowData - Returns a dataset with the data from table
func (tr *TableRef) GetRowData(profile *sqprofile.SQProfile, eList *ExprList, whereExpr Expr) (*DataSet, error) {
	var err error

	err = tr.Table.RLock(profile)
	if err != nil {
		return nil, err
	}

	defer tr.Table.RUnlock(profile)

	tables := NewTableList(profile, []TableRef{*tr})
	// Setup the dataset for the results
	ret, err := NewDataSet(profile, tables, eList)
	if err != nil {
		return nil, err
	}
	ret.usePtrs = !eList.HasAggregateFunc()

	// Get the pointers to the rows based on the conditions
	ptrs, err := tr.Table.GetRowPtrs(profile, whereExpr, RowOrder)
	if err != nil {
		return nil, err
	}

	ret.Vals = make([][]sqtypes.Value, len(ptrs))
	ret.Ptrs = ptrs

	for i, ptr := range ptrs {
		// make sure the ptr points to the correct row
		if tr.Table.rowm[ptr].RowPtr != ptr {
			log.Panic("rowPtr does not match Map index")
		}

		ret.Vals[i], err = eList.Evaluate(profile, EvalFull, tr.Table.rowm[ptr])
		if err != nil {
			return nil, err
		}
	}
	return ret, nil

}

// Validate makes sure that the TableRef points to a valid table
func (tr *TableRef) Validate(profile *sqprofile.SQProfile) error {
	var err error
	if tr.Table == nil {
		// Get the TableDef
		tr.Table, err = GetTable(profile, tr.Name.Name())
		if err != nil {
			return err
		}
		if tr.Table == nil {
			return sqerr.Newf("Table %q does not exist", tr.Name.Name())
		}
	}
	return nil
}
