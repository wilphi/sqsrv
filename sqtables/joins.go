package sqtables

import (
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtypes"
)

//JoinTable defines table like struct for joins
type JoinTable struct {
	Name string
	Tab  *TableDef
	Rows []JoinRow
	Cols []column.Ref
}

// JoinRow defines row definition for joins
type JoinRow struct {
	Ptr       sqptr.SQPtr
	Vals      []sqtypes.Value
	TableName string
}

// GetTableName gets the table name that the JoinRow is based off on.
func (r *JoinRow) GetTableName(profile *sqprofile.SQProfile) string {
	return r.TableName
}

// GetPtr returns the pointer to the given row
func (r *JoinRow) GetPtr(profile *sqprofile.SQProfile) sqptr.SQPtr {
	return r.Ptr
}

// GetColData -
func (r *JoinRow) GetColData(profile *sqprofile.SQProfile, c *column.Ref) (sqtypes.Value, error) {
	if c.Idx < 0 || c.Idx >= len(r.Vals) {
		return nil, sqerr.Newf("Invalid index (%d) for Column in row. Col len = %d", c.Idx, len(r.Vals))
	}
	return r.Vals[c.Idx], nil
}

// GetIdxVal gets the value of the col at the index idx
func (r *JoinRow) GetIdxVal(profile *sqprofile.SQProfile, idx int) (sqtypes.Value, error) {
	if idx < 0 || idx >= len(r.Vals) {
		return nil, sqerr.Newf("Invalid index (%d) for row. Data len = %d", idx, len(r.Vals))
	}
	return r.Vals[idx], nil
}
