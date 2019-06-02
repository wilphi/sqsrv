package sqtables

import (
	"fmt"
	"sort"
	"strings"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqmutex"
	"github.com/wilphi/sqsrv/sqprofile"
)

type tableList struct {
	tables map[string]*TableDef
	sqmutex.SQMutex
}

var _tables *tableList

func init() {
	// setup _tables
	_tables = newTableList()
}

// FindTableDef - Find a table def given the table name
//		protected by a mutex to be concurrency safe
func (tl *tableList) FindTableDef(profile *sqprofile.SQProfile, name string) *TableDef {
	tl.RLock(profile)
	defer tl.RUnlock(profile)
	return tl.tables[strings.ToLower(name)]
}

// CreateTable - Given a table defintion add it to the list of tables
//		protected by a mutex to be concurrency safe
func CreateTable(profile *sqprofile.SQProfile, tab *TableDef) error {
	tableName := tab.GetName(profile)
	// Err if name begins with _ (UnderScore is reserved for system tables)
	if isUnderScore(tableName) {
		return sqerr.New(fmt.Sprintf("Invalid Name: %s - Only system tables may begin with _", tableName))
	}

	// make sure there are more than one cols.
	if tab.NumCol(profile) < 1 {
		return sqerr.New(fmt.Sprintf("Create Table: table must have at least one column"))
	}

	// add to _tables
	_tables.Lock(profile)
	defer _tables.Unlock(profile)

	// Err if there is already a table with the same name
	if _tables.FindTableDef(profile, tableName) != nil {
		return sqerr.New(fmt.Sprintf("Invalid Name: Table %s already exists", tableName))
	}
	_tables.tables[tableName] = tab

	return nil
}

// DropTable - remove table from list of tables
//		protected by a mutex to be concurrency safe
func DropTable(profile *sqprofile.SQProfile, name string) error {

	name = strings.ToLower(name)
	// Err is name begins with _
	if isUnderScore(name) {
		return sqerr.New(fmt.Sprintf("Invalid Name: %s - Unable to drop system tables", name))
	}

	_tables.Lock(profile)
	defer _tables.Unlock(profile)

	// Err if table does not exist
	tab := _tables.FindTableDef(profile, name)
	if tab == nil {
		return sqerr.New(fmt.Sprintf("Invalid Name: Table %s does not exist", name))
	}
	// Make sure that no one else is changing the table
	tab.Lock(profile)
	// Unlock when done to make sure the lock tracking is correct
	defer tab.Unlock(profile)
	// remove from _tables
	_tables.tables[strings.ToLower(name)] = nil

	//Clear out the values
	tab.rowm = nil
	tab.tableCols = nil
	tab.tableName = ""

	return nil
}

// newTableList - Initialize a new TableList
func newTableList() *tableList {
	return &tableList{tables: make(map[string]*TableDef), SQMutex: sqmutex.NewSQMutex("TableList: ")}
}

// ListTables returns a sorted list of tablenames
func ListTables(profile *sqprofile.SQProfile) []string {
	_tables.RLock(profile)
	defer _tables.RUnlock(profile)
	var tNames []string

	for tab := range _tables.tables {
		if tab != "" {
			if _tables.tables[tab] != nil {
				tNames = append(tNames, tab)

			}
		}
	}
	sort.Strings(tNames)
	return tNames

}

// ListAllTables returns a sorted list of tablenames including dropped tables
func ListAllTables(profile *sqprofile.SQProfile) []string {
	_tables.RLock(profile)
	defer _tables.RUnlock(profile)
	tNames := make([]string, len(_tables.tables))
	i := 0
	for tab := range _tables.tables {
		tNames[i] = tab
		i++
	}
	sort.Strings(tNames)
	return tNames

}

// LockAll write locks the tableList and all tables in it
func (tl *tableList) LockAll(profile *sqprofile.SQProfile) {
	tl.Lock(profile)

	for _, tab := range tl.tables {
		if tab != nil {
			tab.Lock(profile)
		}
	}

}

//UnlockAll write unlocks the tableList and all tables in it
func (tl *tableList) UnlockAll(profile *sqprofile.SQProfile) {
	for _, tab := range tl.tables {
		if tab != nil {
			tab.Unlock(profile)
		}
	}
	tl.Unlock(profile)
}
