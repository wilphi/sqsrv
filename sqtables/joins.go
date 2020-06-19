package sqtables

import (
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtypes"
)

//JoinTable defines table like struct for joins
type JoinTable struct {
	TR   TableRef
	Rows []JoinRow
	Cols []column.Ref
}

// JoinRow defines row definition for joins
type JoinRow struct {
	Ptr       sqptr.SQPtr
	Vals      []sqtypes.Value
	TableName *moniker.Moniker
}

// GetTableName gets the table name that the JoinRow is based off on.
func (r *JoinRow) GetTableName(profile *sqprofile.SQProfile) string {
	return r.TableName.Show()
}

// GetPtr returns the pointer to the given row
func (r *JoinRow) GetPtr(profile *sqprofile.SQProfile) sqptr.SQPtr {
	return r.Ptr
}

// ColVal -
func (r *JoinRow) ColVal(profile *sqprofile.SQProfile, c *column.Ref) (sqtypes.Value, error) {
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

////////////////////////////////////

// NullRow defines a row that always has values of null. used for outer joins
type NullRow struct {
	TableName *moniker.Moniker
}

// GetTableName gets the table name that the JoinRow is based off on.
func (r *NullRow) GetTableName(profile *sqprofile.SQProfile) string {
	return r.TableName.Show()
}

// GetPtr returns the pointer to the given row
func (r *NullRow) GetPtr(profile *sqprofile.SQProfile) sqptr.SQPtr {
	return 0
}

// ColVal -
func (r *NullRow) ColVal(profile *sqprofile.SQProfile, c *column.Ref) (sqtypes.Value, error) {
	return sqtypes.NewSQNull(), nil
}

// GetIdxVal gets the value of the col at the index idx
func (r *NullRow) GetIdxVal(profile *sqprofile.SQProfile, idx int) (sqtypes.Value, error) {
	return sqtypes.NewSQNull(), nil

}
