package sqtables

import (
	e "github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtypes"
)

// RowDef - in memory construct for rows
type RowDef struct {
	RowID      int64
	Data       []sqtypes.Value
	isModified bool
	isDeleted  bool
	offset     int64
	alloc      int64
	size       int64
	table      *TableDef
	ColNum     int
}

// Methods

// UpdateRow updates the values of the row
func (r *RowDef) UpdateRow(profile *sqprofile.SQProfile, cols []string, vals []sqtypes.Value) error {
	if len(cols) != len(vals) {
		return e.Newf("The Number of Columns (%d) does not match the number of Values (%d)", len(cols), len(vals))
	}

	for i, col := range cols {
		colDef := r.table.FindColDef(profile, col)
		if colDef == nil {
			return e.New("Column (" + col + ") does not exist in table (" + r.table.GetName(profile) + ")")
		}
		if colDef.IsNotNull && vals[i].IsNull() {
			return e.Newf("Column %q in Table %q can not be NULL", col, r.table.tableName)
		}
		if colDef.ColType != vals[i].Type() && !vals[i].IsNull() {
			return e.Newf("Type Mismatch: Column %s in Table %s has a type of %s, Unable to set value of type %s", colDef.ColName, r.table.tableName, colDef.ColType, vals[i].Type())
		}
		r.Data[colDef.Idx] = vals[i]

	}
	r.isModified = true

	return nil
}

// CreateRow -
func CreateRow(profile *sqprofile.SQProfile, rowID int64, table *TableDef, cols []string, vals []sqtypes.Value) (*RowDef, error) {
	colNum := len(table.tableCols)
	row := RowDef{
		RowID:      rowID,
		Data:       make([]sqtypes.Value, colNum),
		isModified: true,
		isDeleted:  false,
		table:      table,
		ColNum:     colNum,
		offset:     -1,
		alloc:      -1,
		size:       0,
	}

	if colNum < len(cols) {
		return nil, e.New("More columns are being set than exist in table definition")
	}
	if len(cols) != len(vals) {
		return nil, e.Newf("The Number of Columns (%d) does not match the number of Values (%d)", len(cols), len(vals))
	}

	for i, col := range cols {
		colDef := row.table.FindColDef(profile, col)
		if colDef == nil {
			return nil, e.New("Column (" + col + ") does not exist in table (" + table.GetName(profile) + ")")
		}
		if colDef.IsNotNull && vals[i].IsNull() {
			return nil, e.Newf("Column %q in Table %q can not be NULL", col, row.table.tableName)
		}
		if colDef.ColType != vals[i].Type() && !vals[i].IsNull() {
			return nil, e.Newf("Type Mismatch: Column %s in Table %s has a type of %s, Unable to set value of type %s", colDef.ColName, row.table.tableName, colDef.ColType, vals[i].Type())
		}

		row.Data[colDef.Idx] = vals[i]

	}
	// Validate NotNull cols
	for i, val := range row.Data {

		if (val == nil || val.IsNull()) && table.tableCols[i].IsNotNull {
			return nil, e.Newf("Column %q in Table %q can not be NULL", table.tableCols[i].ColName, table.tableName)
		}
		if val == nil {
			row.Data[i] = sqtypes.NewSQNull()
		}
	}

	return &row, nil
}

// GetColData -
func (r *RowDef) GetColData(profile *sqprofile.SQProfile, c *ColDef) (sqtypes.Value, error) {

	if r.isDeleted {
		return nil, e.New("Referenced Row has been deleted")
	}
	idx, ctype := r.table.FindCol(profile, c.ColName)
	if idx < 0 {
		//error
		return nil, e.Newf("%s not found in table %s", c.ColName, r.table.GetName(profile))
	}
	if c.ColType != ctype {
		//type error
		return nil, e.Newf("%s's type of %s does not match table definition for table %s", c.ColName, c.ColType, r.table.GetName(profile))

	}
	return r.Data[idx], nil
}

// SetStorage sets the disk storage parameters of the row. With the offset and alloc you can
//		find where exactly in a file this row is stored. Size indicates how much of the allocated
//		block the data for this row takes on disk
func (r *RowDef) SetStorage(profile *sqprofile.SQProfile, offset, alloc, size int64) {
	r.offset = offset
	r.alloc = alloc
	r.size = size
}

//Delete soft deletes the row
func (r *RowDef) Delete(profile *sqprofile.SQProfile) {
	r.isDeleted = true
	r.isModified = true
}
