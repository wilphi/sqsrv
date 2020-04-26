package redo

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqptr"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
)

// Constants that identify the logstatement type
const (
	IDCreateDDL  = 1
	IDInsertRows = 2
	IDUpdateRows = 3
	IDDeleteRows = 4
	IDDropDDL    = 5
)

// LogStatement - Interface to represent each type of redo statement
type LogStatement interface {
	Recreate(profile *sqprofile.SQProfile) error
	Identify(ID uint64) string
	Encode() *sqbin.Codec
	Decode(*sqbin.Codec)
}

// CreateLogMsg sends a logstatement plus a channel to reply with a possible error
func CreateLogMsg(resp chan error, stmt LogStatement) LogMsg {
	enc := stmt.Encode()
	return LogMsg{buffer: enc.Bytes(), respond: resp}
}

// CreateDDL - Transaction Recording for Create Statement
type CreateDDL struct {
	TableName string
	Cols      []sqtables.ColDef
	//	ID        uint64
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (c *CreateDDL) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.Writebyte(IDCreateDDL)
	// Id of transaction statement
	//	enc.WriteUint64(c.ID)

	enc.WriteString(c.TableName)
	encColDef(enc, c.Cols)
	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (c *CreateDDL) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDCreateDDL {
		log.Panic("Found wrong statement type. Expecting IDCreateDDL")
	}
	// Id of transaction statement
	//	c.ID = dec.ReadUint64()

	c.TableName = dec.ReadString()
	c.Cols = decColDef(dec)
}

// Recreate - reprocess the recorded transaction log SQL statement to restore the database
func (c *CreateDDL) Recreate(profile *sqprofile.SQProfile) error {

	table := sqtables.CreateTableDef(c.TableName, c.Cols...)
	err := sqtables.CreateTable(profile, table)

	profile.VerifyNoLocks()
	return err
}

// Identify - returns a short string to identify the transaction log statement
func (c *CreateDDL) Identify(ID uint64) string {
	return fmt.Sprintf("#%d - CREATE TABLE %s", ID, c.TableName)
}

// NewCreateDDL returns a logstatement that is a CREATE TABLE
func NewCreateDDL(name string, cols []sqtables.ColDef) *CreateDDL {
	return &CreateDDL{TableName: name, Cols: cols}
}

// InsertRows - Redo recording for Insert statement
type InsertRows struct {
	TableName string
	Cols      []string
	Data      [][]sqtypes.Value
	RowPtrs   sqptr.SQPtrs
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (i *InsertRows) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.Writebyte(IDInsertRows)

	enc.WriteString(i.TableName)

	// encode the Cols
	enc.WriteArrayString(i.Cols)
	enc.WriteSQPtrs(i.RowPtrs)
	encodeData(enc, i.Data)

	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (i *InsertRows) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDInsertRows {
		log.Panic("Found wrong statement type. Expecting IDInsertRows")
	}

	// Id of transaction statement
	//	i.ID = dec.ReadUint64()

	i.TableName = dec.ReadString()

	// encode the Cols
	i.Cols = dec.ReadArrayString()
	i.RowPtrs = dec.ReadSQPtrs()
	i.Data = decodeData(dec)
}

// Recreate - reprocess the recorded transaction log SQL statement to restore the database
func (i *InsertRows) Recreate(profile *sqprofile.SQProfile) error {

	// make sure there is a valid table
	tab, err := sqtables.GetTable(profile, i.TableName)
	if err != nil {
		return err
	}
	if tab == nil {
		return sqerr.New("Table " + i.TableName + " does not exist")
	}
	tables := sqtables.NewTableListFromTableDef(profile, tab)

	colList := sqtables.NewColListNames(i.Cols)
	if err := colList.Validate(profile, tables); err != nil {
		return err
	}
	dataSet, err := sqtables.NewDataSet(profile, tables, sqtables.ColsToExpr(colList), nil)
	if err != nil {
		return err
	}
	dataSet.Vals = i.Data
	dataSet.Ptrs = i.RowPtrs

	_, err = tab.AddRows(profile, dataSet)

	profile.VerifyNoLocks()
	return err
}

// Identify - returns a short string to identify the transaction log statement
func (i *InsertRows) Identify(ID uint64) string {

	return fmt.Sprintf("#%d - INSERT INTO %s : Rows = %d", ID, i.TableName, len(i.Data))
}

// NewInsertRows -  returns a logstatement that is a INSERT INTO
func NewInsertRows(TableName string, cols []string, data [][]sqtypes.Value, ptrs sqptr.SQPtrs) *InsertRows {
	val := &InsertRows{TableName: TableName, Cols: cols, Data: data, RowPtrs: ptrs}
	return val
}

// UpdateRows - Redo recording for Update statement
type UpdateRows struct {
	TableName string
	Cols      []string
	EList     *sqtables.ExprList
	RowPtrs   sqptr.SQPtrs
	//	ID        uint64
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (u *UpdateRows) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.Writebyte(IDUpdateRows)
	// Id of transaction statement
	//	enc.WriteUint64(u.ID)

	enc.WriteString(u.TableName)

	// encode the Cols
	enc.WriteArrayString(u.Cols)
	enc.WriteSQPtrs(u.RowPtrs)
	tmp := u.EList.Encode()
	enc.Write(tmp.Bytes())
	//	data := [][]sqtypes.Value{u.Vals}
	//	encodeData(enc, data)
	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (u *UpdateRows) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDUpdateRows {
		log.Panic("Found wrong statement type. Expecting IDUpdateRows")
	}

	// Id of transaction statement
	//	u.ID = dec.ReadUint64()

	u.TableName = dec.ReadString()

	// encode the Cols
	u.Cols = dec.ReadArrayString()
	u.RowPtrs = dec.ReadSQPtrs()
	u.EList = sqtables.DecodeExprList(dec)
	//	data := decodeData(dec)
	//	u.Vals = data[0]
}

// Recreate - reprocess the recorded transaction log SQL statement to restore the database
func (u *UpdateRows) Recreate(profile *sqprofile.SQProfile) error {
	// make sure there is a valid table
	tab, err := sqtables.GetTable(profile, u.TableName)
	if err != nil {
		return err
	}
	if tab == nil {
		return sqerr.New("Table " + u.TableName + " does not exist")
	}

	return tab.UpdateRowsFromPtrs(profile, u.RowPtrs, u.Cols, u.EList)
}

// Identify - returns a short string to identify the transaction log statement
func (u *UpdateRows) Identify(ID uint64) string {
	return fmt.Sprintf("#%d - UPDATE  %s : Rows = %d", ID, u.TableName, len(u.RowPtrs))
}

// NewUpdateRows -  returns a logstatement that is a UPDATE statement
func NewUpdateRows(TableName string, cols []string, eList *sqtables.ExprList, ptrs sqptr.SQPtrs) *UpdateRows {
	val := &UpdateRows{TableName: TableName, Cols: cols, EList: eList, RowPtrs: ptrs}
	return val

}

// DeleteRows - Redo recording for Delete statement
type DeleteRows struct {
	TableName string
	RowPtrs   sqptr.SQPtrs
	//	ID        uint64
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (d *DeleteRows) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.Writebyte(IDDeleteRows)
	// Id of transaction statement
	//	enc.WriteUint64(d.ID)

	enc.WriteString(d.TableName)

	// encode the Cols
	enc.WriteSQPtrs(d.RowPtrs)

	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (d *DeleteRows) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDDeleteRows {
		log.Panic("Found wrong statement type. Expecting IDDeleteRows")
	}

	// Id of transaction statement
	//	d.ID = dec.ReadUint64()

	d.TableName = dec.ReadString()

	// encode the Cols
	d.RowPtrs = dec.ReadSQPtrs()
}

// Recreate - reprocess the recorded transaction log SQL statement to restore the database
func (d *DeleteRows) Recreate(profile *sqprofile.SQProfile) error {
	// make sure there is a valid table
	tab, err := sqtables.GetTable(profile, d.TableName)
	if err != nil {
		return err
	}
	if tab == nil {
		return sqerr.New("Table " + d.TableName + " does not exist")
	}
	err = tab.DeleteRowsFromPtrs(profile, d.RowPtrs, sqtables.SoftDelete)
	profile.VerifyNoLocks()

	return err

}

// Identify - returns a short string to identify the transaction log statement
func (d *DeleteRows) Identify(ID uint64) string {
	return fmt.Sprintf("#%d - DELETE FROM  %s : Rows = %d", ID, d.TableName, len(d.RowPtrs))
}

// NewDeleteRows -
func NewDeleteRows(TableName string, ptrs sqptr.SQPtrs) *DeleteRows {
	val := &DeleteRows{TableName: TableName, RowPtrs: ptrs}
	return val
}

// DecodeStatementType is used to define a function prototype for functions
//    that will hook into DecodeStatement to extend what types of LogStatments
//    can be decoded. This should only be used for testing
type DecodeStatementType = func(b byte) LogStatement

// DecodeStatementHook is the variable used to store a function of type DecodeStatementType
//    it is used to extend what type of LogStatements can be decoded. This should only be
//    used for testing
var DecodeStatementHook DecodeStatementType

// DecodeStatement determines the correct type of statement to decode
//    and has that type do the actual decoding
func DecodeStatement(dec *sqbin.Codec) LogStatement {
	// the first byte should be the statement type
	var stmt LogStatement
	stype := dec.PeekByte()
	switch stype {
	case IDCreateDDL:
		stmt = &CreateDDL{}
	case IDInsertRows:
		stmt = &InsertRows{}
	case IDUpdateRows:
		stmt = &UpdateRows{}
	case IDDeleteRows:
		stmt = &DeleteRows{}
	case IDDropDDL:
		stmt = &DropDDL{}
	default:
		if DecodeStatementHook != nil {
			stmt = DecodeStatementHook(stype)
		} else {
			log.Panicf("Attempting to decode unknown LogStatement type %d", stype)
		}
	}
	stmt.Decode(dec)
	return stmt
}
func encColDef(enc *sqbin.Codec, cols []sqtables.ColDef) {
	// encode size of slice
	enc.WriteInt(len(cols))

	for _, col := range cols {
		col.Encode(enc)
	}
}
func decColDef(dec *sqbin.Codec) []sqtables.ColDef {
	// encode size of slice
	lCols := dec.ReadInt()
	cols := make([]sqtables.ColDef, lCols)

	for i := 0; i < lCols; i++ {
		cols[i].Decode(dec)
	}
	return cols
}

func encodeData(enc *sqbin.Codec, data [][]sqtypes.Value) {
	// write the number of rows
	enc.WriteInt(len(data))

	for _, row := range data {
		// write number of values in the row
		enc.WriteInt(len(row))
		for _, val := range row {
			val.Write(enc)
		}
	}
}

func decodeData(dec *sqbin.Codec) [][]sqtypes.Value {
	// Get the number of rows
	l := dec.ReadInt()

	data := make([][]sqtypes.Value, l)
	for i := 0; i < l; i++ {
		//get number of values in row
		lrow := dec.ReadInt()
		row := make([]sqtypes.Value, lrow)
		for j := 0; j < lrow; j++ {
			row[j] = sqtypes.ReadValue(dec)
		}
		data[i] = row
	}
	return data
}

// DropDDL - Transaction Recording for Drop Table Statement
type DropDDL struct {
	TableName string
	ID        uint64
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (d *DropDDL) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.Writebyte(IDDropDDL)
	// Id of transaction statement
	enc.WriteUint64(d.ID)

	enc.WriteString(d.TableName)
	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (d *DropDDL) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDDropDDL {
		log.Panic("Found wrong statement type. Expecting IDDropDDL")
	}
	// Id of transaction statement
	d.ID = dec.ReadUint64()

	d.TableName = dec.ReadString()
}

// Recreate - reprocess the recorded transaction log SQL statement to restore the database
func (d *DropDDL) Recreate(profile *sqprofile.SQProfile) error {

	err := sqtables.DropTable(profile, d.TableName)

	profile.VerifyNoLocks()
	return err
}

// Identify - returns a short string to identify the transaction log statement
func (d *DropDDL) Identify(ID uint64) string {
	return fmt.Sprintf("#%d - DROP TABLE %s", ID, d.TableName)
}

// NewDropDDL returns a logstatement that is a CREATE TABLE
func NewDropDDL(name string) *DropDDL {
	return &DropDDL{TableName: name}
}
