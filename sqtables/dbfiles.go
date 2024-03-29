package sqtables

import (
	"encoding/gob"
	"io"
	"os"
	"sync"
	"time"

	"github.com/wilphi/sqsrv/files"
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqptr"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/transid"
)

// Manages the Database Files for permanent storage

var dbDirectory = "./dbfiles"
var infoFile = "/info.sqd"

// DBInfo stores toplevel information about database
type DBInfo struct {
	LastTransID uint64
	Tables      []string
}

// DBTable stores table information
type DBTable struct {
	TableName  string
	Cols       []column.Def
	NRows      int
	NextRowPtr uint64
}

// DBRow -
type DBRow struct {
	RowPtr sqptr.SQPtr
	Data   []sqtypes.Value
}

// the number times a file can be renumbered
const maxFiles = 100

var doDirOnce sync.Once

func init() {
	gob.Register(sqtypes.SQString{})
	gob.Register(sqtypes.SQInt{})
	gob.Register(sqtypes.SQBool{})
	gob.Register(sqtypes.SQNull{})
}

// SetDBDir sets the path to the directory that contains the database files
// It may be an absolute or relative path. This should only be set once before
// the database is read/written to.
func SetDBDir(path string) {
	doDirOnce.Do(func() {

		dbDirectory = path
		stat, err := os.Stat(path)
		if err != nil {
			log.Fatalf("Error with dbfiles path %s", path)
		}
		if !stat.IsDir() {
			log.Fatalf("dbfiles path %s is not a directory", path)
		}
	})

}

// WriteDB - writes database to files
func WriteDB(profile *sqprofile.SQProfile) error {
	// Make sure database is paused or stopped

	// Get locks on Catalog and each table
	LockCatalog(profile)
	defer UnlockCatalog(profile)

	// Get the last transaction
	id := transid.GetTransID()

	// get the list of tables currently in use
	tables, err := CatalogTables(profile)
	if err != nil {
		return err
	}
	info := DBInfo{LastTransID: id, Tables: tables}

	err = writeDBInfo(profile, info)
	if err != nil {
		log.Error("Unable to write to info file", err)
	}

	// now get all of the tables including those that have been dropped
	tables, err = CatalogAllTables(profile)
	if err != nil {
		return err
	}
	for _, tableName := range tables {
		err = writeDBTableInfo(profile, tableName)
		if err != nil {
			log.Error("Unable to write table info for "+tableName, err)
			return err
		}
		err = writeDBTableData(profile, tableName)
		if err != nil {
			log.Error("Unable to write datafile for "+tableName, err)
			return err
		}

	}
	log.Infof("Checkpoint Completed. TransactionId = %d", transid.GetTransID())
	return nil
}
func writeDBInfo(profile *sqprofile.SQProfile, d DBInfo) error {

	file, err := os.OpenFile(dbDirectory+infoFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Panic(err)
	}
	enc := gob.NewEncoder(file)
	defer file.Close()

	err = enc.Encode(&d)
	return err
}

func writeDBTableInfo(profile *sqprofile.SQProfile, tName string) error {
	fileName := dbDirectory + "/" + tName + ".sqt"

	td, err := GetTable(profile, tName)
	if err != nil {
		return err
	}
	if td == nil {
		err = files.NumberFile(fileName, maxFiles)
		if err != nil {
			return err
		}
		return nil
	}

	tab := DBTable{TableName: tName, Cols: td.tableCols, NRows: len(td.rowm), NextRowPtr: *td.nextRowID}
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		//	log.Fatal(err)
		log.Panic(err)
	}
	enc := gob.NewEncoder(file)
	defer file.Close()

	err = enc.Encode(&tab)

	return err

}

func deleteBlock(file *os.File, offset, alloc int64) error {
	unused := sqbin.NewCodec(nil)

	err := writeCodecAt(file, offset, alloc, 0, unused)
	return err
}
func writeDBTableData(profile *sqprofile.SQProfile, tName string) error {
	var err error
	var deletePtrs sqptr.SQPtrs
	fileName := dbDirectory + "/" + tName + ".sqd"
	td, err := GetTable(profile, tName)
	if err != nil {
		return err
	}
	if td == nil {
		err := files.NumberFile(fileName, maxFiles)
		if err != nil {
			return err
		}
		return nil
	}

	datafile, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		//	log.Fatal(err)
		log.Panic(err)
	}
	defer datafile.Close()

	var databuff *sqbin.Codec

	nextOffset := td.nextOffset

	// get the ordered list of RowPtrs
	list, _ := td.GetRowPtrs(profile, nil, true)

	for _, RowPtr := range list {
		rw := td.rowm[sqptr.SQPtr(RowPtr)]
		row := rw.(*RowDef)
		if !row.isModified {
			continue
		}
		if row.isDeleted {
			// mark the row as deleted if it exists
			deleteBlock(datafile, row.offset, row.alloc)
			deletePtrs = append(deletePtrs, row.RowPtr)
			continue
		}
		dbrow := DBRow{RowPtr: row.RowPtr, Data: row.Data}
		databuff = writeRow(&dbrow)
		bufflen := databuff.Len()

		// Add 2* IntSize bytes for storing the values of (alloc, size) on disk
		alloc := NextLargerBlock(bufflen + sqbin.IntSize*2)

		if row.alloc <= 0 {
			// this row has not been stored to disk yet

			//set the disk location in memory for future updates
			row.alloc = alloc
			row.offset = nextOffset
			nextOffset += alloc
		}
		if alloc > row.alloc {
			// Not enough space at current location, put it at end of file

			//First mark old location as unused

			err = deleteBlock(datafile, row.offset, row.alloc)
			if err != nil {
				return err
			}
			//set the new disk location
			row.alloc = alloc
			row.offset = nextOffset
			nextOffset += alloc

		}
		row.size = int64(bufflen)

		err = writeCodecAt(datafile, row.offset, row.alloc, row.size, databuff)
		if err != nil {
			return err
		}
		databuff.Reset()

	}
	td.nextOffset = nextOffset

	// reset the isModified flag
	for _, rw := range td.rowm {
		row := rw.(*RowDef)
		row.isModified = false
	}

	err = td.HardDeleteRowsFromPtrs(profile, deletePtrs)
	if err != nil {
		return err
	}

	return nil

}

func writeCodecAt(file *os.File, offset, alloc, size int64, enc *sqbin.Codec) error {
	enc.Insert(alloc, size)

	n, err := file.WriteAt(enc.Bytes(), offset)
	if err != nil {
		return err
	}
	if n != enc.Len() {
		sqerr.NewInternalf("Len of disk write (%d) does not match expected size (%d)", n, enc.Len())
	}
	return nil
}

func readCodecAt(file *os.File, offset int64) (int64, int64, *sqbin.Codec, error) {
	var store = make([]byte, sqbin.IntSize*2)

	_, err := file.ReadAt(store, offset)
	if err != nil {
		return -1, -1, nil, err
	}
	dec := sqbin.NewCodec(store)
	alloc := dec.ReadInt64()

	size := dec.ReadInt64()
	store = make([]byte, size)
	_, err = file.ReadAt(store, offset+sqbin.IntSize*2)
	if err != nil {
		return -1, -1, nil, err
	}

	ret := sqbin.NewCodec(store)
	return alloc, size, ret, nil
}

func writeRow(row *DBRow) *sqbin.Codec {
	var enc sqbin.Codec
	// Write the ID first
	enc.WriteUint64(uint64(row.RowPtr))

	// Write the number of values
	enc.WriteInt(len(row.Data))
	for _, val := range row.Data {
		val.Write(&enc)
	}
	return &enc
}

func readRow(dec *sqbin.Codec) *DBRow {
	row := &DBRow{}
	// Get the ID
	row.RowPtr = sqptr.SQPtr(dec.ReadUint64())

	// Get the number of Values
	vlen := dec.ReadInt()

	row.Data = make([]sqtypes.Value, vlen)

	for i := 0; i < vlen; i++ {
		row.Data[i] = sqtypes.ReadValue(dec)
	}

	return row
}

// NextLargerBlock returns the next larger block size for disk storage
// Max block size is 1Mb (1048576 bytes)
func NextLargerBlock(num int) int64 {

	for i := 64; i < 1048577; i *= 2 {
		if num < i {
			return int64(i)
		}
	}

	return 0
}

// ReadDB - reads database files into memory
func ReadDB(profile *sqprofile.SQProfile) error {
	log.Info("Opening Database...")
	start := time.Now()

	// Get locks on Catalog and each table
	LockCatalog(profile)
	defer UnlockCatalog(profile)

	catTables, err := CatalogTables(profile)
	if err != nil {
		return err
	}
	if len(catTables) != 0 {
		log.Panic("To read the database from file, memory must be empty")
	}

	info, err := readDBInfo(profile)
	if err != nil {
		log.Panic("Unable to read from dbinfo file", err)
	}
	if info == nil {
		log.Warn("No Database to read from. Creating new database...")
		return nil
	}
	// get the transid
	transid.SetTransID(info.LastTransID)

	for _, tableName := range info.Tables {
		if tableName == "" {
			continue
		}
		log.Info("Loading " + tableName)

		tab, err := readDBTableInfo(profile, tableName)
		if err != nil {
			log.Panicf("Unable to read table info for %s: %s", tableName, err)
		}
		err = CreateTable(profile, tab)
		if err != nil {
			log.Panicf("Unable to create table %s: %s", tableName, err)
		}
		err = tab.Lock(profile)
		if err != nil {
			return err
		}
		err = readDBTableData(profile, tab)
		if err != nil && err != io.EOF {
			log.Panicf("Unable to read table data for %s: %s", tableName, err)
		}
	}
	length := time.Since(start)
	log.Infof("Time spend opening Database: %v", length)

	return err
}

func readDBInfo(profile *sqprofile.SQProfile) (*DBInfo, error) {

	file, err := os.Open(dbDirectory + infoFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		//	log.Fatal(err)
		log.Panic(err)
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	d := DBInfo{}
	err = dec.Decode(&d)
	return &d, err

}

func readDBTableInfo(profile *sqprofile.SQProfile, tName string) (*TableDef, error) {
	file, err := os.Open(dbDirectory + "/" + tName + ".sqt")
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	dec := gob.NewDecoder(file)

	tab := DBTable{}
	err = dec.Decode(&tab)
	if err != nil {
		log.Panic(err)
	}

	tabDef := CreateTableDef(tName, tab.Cols)
	nn := tab.NextRowPtr
	tabDef.nextRowID = &nn
	return tabDef, err

}

func readDBTableData(profile *sqprofile.SQProfile, tab *TableDef) error {
	var offset, alloc, size int64
	var databuff *sqbin.Codec

	datafile, err := os.Open(dbDirectory + "/" + tab.GetName(profile) + ".sqd")
	if err != nil {
		log.Panic(err)
	}
	defer datafile.Close()

	// get Col Names
	colNames := tab.GetColNames(profile)

	offset = 0
	for {
		alloc, size, databuff, err = readCodecAt(datafile, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil
		}
		log.Debugf("Offset = %d, Alloc = %d, Size=%d", offset, alloc, size)
		// valid disk block if size >0, otherwise skip this block
		if size > 0 {
			dbrow := readRow(databuff)
			log.Debugf("RowPtr = %d", dbrow.RowPtr)
			row, err := CreateRow(profile, dbrow.RowPtr, tab, colNames, dbrow.Data)
			if err != nil {
				return err
			}
			row.SetStorage(profile, offset, alloc, size)
			row.isModified = false
			tab.rowm[row.RowPtr] = row
			if !row.isDeleted {
				tab.rowCnt++
			}
		}
		offset += alloc
	}
	tab.nextOffset = offset

	return nil
}
