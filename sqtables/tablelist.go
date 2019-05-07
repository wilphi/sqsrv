package sqtables

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/wilphi/sqsrv/sqmutex"
	"github.com/wilphi/sqsrv/sqprofile"
)

type tableList struct {
	tables []*TableDef
	sqmutex.SQMutex
}

var _tables *tableList

func init() {
	// setup _tables

	//	tab := CreateTableDef("_tables", []ColDef{{"name", t.TypeString, false, 0, true}, {"type", t.TypeString, false, 1, true}}...)

	_tables = newTableList()
	//	_tables.addSystemTable(tab)

	//	fmt.Println(_tables.ToString())
}

// findTableDef - Find a table def given the table name
//		protected by a mutex to be concurrency safe
func (tl *tableList) FindTableDef(profile *sqprofile.SQProfile, name string) *TableDef {
	tl.RLock(profile)
	defer tl.RUnlock(profile)
	for _, t := range tl.tables {
		if t.tableName == strings.ToLower(name) {

			return t
		}
	}
	return nil
}

// addSystemTable - Adds a system table definition to the list of tables.
func (tl *tableList) addSystemTable(profile *sqprofile.SQProfile, tab *TableDef) {

	// Err if name DOES NOT begin with _ (UnderScore is reserved for system tables)
	if !isUnderScore(tab.GetName(profile)) {
		log.Fatalf("Invalid Name: %s - System tables must begin with _", tab.GetName(profile))
	}

	// make sure there are more than one cols.
	if tab.NumCol(profile) <= 1 {
		log.Fatalf("System Table: table %q must have at least one column", tab.GetName(profile))
	}
	// Err if there is already a table with the same name
	if tl.FindTableDef(profile, tab.GetName(profile)) != nil {
		log.Fatalf("Invalid Name: Table %s already exists", tab.GetName(profile))
	}

	// add to tablelist
	// Err if there is already a table with the same name - part of Check/Lock/Check pattern
	if tl.FindTableDef(profile, tab.GetName(profile)) != nil {
		log.Fatalf("Invalid Name: Table %s already exists", tab.GetName(profile))
	}
	tl.tables = append(tl.tables, tab)

}

// CreateTable - Given a table defintion add it to the list of tables
//		protected by a mutex to be concurrency safe
func CreateTable(profile *sqprofile.SQProfile, tab *TableDef) (bool, error) {
	// Err if name begins with _ (UnderScore is reserved for system tables)
	if isUnderScore(tab.GetName(profile)) {
		return false, fmt.Errorf("Invalid Name: %s - Only system tables may begin with _", tab.GetName(profile))
	}

	// make sure there are more than one cols.
	if tab.NumCol(profile) < 1 {
		return false, fmt.Errorf("Create Table: table must have at least one column")
	}

	// Err if there is already a table with the same name
	_tables.RLock(profile)
	if _tables.FindTableDef(profile, tab.GetName(profile)) != nil {
		_tables.RUnlock(profile)
		return false, fmt.Errorf("Invalid Name: Table %s already exists", tab.GetName(profile))
	}
	_tables.RUnlock(profile)

	// add to _tables
	_tables.Lock(profile)
	defer _tables.Unlock(profile)

	// Err if there is already a table with the same name - part of Check/Lock/Check pattern
	if _tables.FindTableDef(profile, tab.GetName(profile)) != nil {
		return false, fmt.Errorf("Invalid Name: Table %s already exists", tab.GetName(profile))
	}
	_tables.tables = append(_tables.tables, tab)

	fmt.Println(_tables.ToString(profile))

	return true, nil
}

// DropTable - remove table from list of tables
//		protected by a mutex to be concurrency safe
func DropTable(profile *sqprofile.SQProfile, name string) (bool, error) {
	var i int

	// Err is name begins with _
	if isUnderScore(name) {
		return false, fmt.Errorf("Invalid Name: %s - Unable to drop system tables", name)
	}

	_tables.Lock(profile)
	defer _tables.Unlock(profile)

	// Err if table does not exist
	if _tables.FindTableDef(profile, name) == nil {

		return false, fmt.Errorf("Invalid Name: Table %s does not exist", name)
	}
	// remove from _tables
	_tables.tables = append(_tables.tables[:i], _tables.tables[i+1:]...)
	//fmt.Println(tableDefString(_tables))

	return true, nil
}

// newTableList - Initialize a new TableList
func newTableList() *tableList {
	return &tableList{SQMutex: sqmutex.NewSQMutex("TableList: ")}
}

// ToString -
func (tl *tableList) ToString(profile *sqprofile.SQProfile) string {
	tl.RLock(profile)
	defer tl.RUnlock(profile)
	rt := ""
	for i, tab := range tl.tables {
		rt += "[" + strconv.Itoa(i) + "]" + tab.ToString(profile)
	}
	return rt
}

// ListTables returns a list of tablenames
func ListTables(profile *sqprofile.SQProfile) []string {
	_tables.RLock(profile)
	defer _tables.RUnlock(profile)
	tNames := make([]string, len(_tables.tables))
	for i, tab := range _tables.tables {
		tNames[i] = tab.tableName
	}
	return tNames

}

// LockAll write locks the tableList and all tables in it
func (tl *tableList) LockAll(profile *sqprofile.SQProfile) {
	tl.Lock(profile)

	for _, tab := range tl.tables {
		tab.Lock(profile)
	}

}

//UnlockAll write unlocks the tableList and all tables in it
func (tl *tableList) UnlockAll(profile *sqprofile.SQProfile) {
	for _, tab := range tl.tables {
		tab.Unlock(profile)
	}
	tl.Unlock(profile)
}
