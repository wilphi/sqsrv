package sqtables

import (
	"sort"
	"strings"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqmutex"
	"github.com/wilphi/sqsrv/sqprofile"
)

type tableCatalog struct {
	tables map[string]*TableDef
	*sqmutex.SQMtx
}

var _Catalog *tableCatalog

func init() {
	// setup _Catalog
	_Catalog = newTableCatalog()
}

// FindTableDefO - Find a table def given the table name
//		protected by a mutex to be concurrency safe
func (tl *tableCatalog) FindTableDef(profile *sqprofile.SQProfile, name string) (*TableDef, error) {
	err := tl.RLock(profile)
	if err != nil {
		return nil, err
	}
	defer tl.RUnlock(profile)
	return tl.tables[strings.ToLower(name)], nil
}

// CreateTable - Given a table defintion add it to the list of tables
//		protected by a mutex to be concurrency safe
func CreateTable(profile *sqprofile.SQProfile, tab *TableDef) error {
	tableName := tab.GetName(profile)
	// Err if name begins with _ (UnderScore is reserved for system tables)
	if isUnderScore(tableName) {
		return sqerr.Newf("Invalid Name: %s - Only system tables may begin with _", tableName)
	}

	if tableName == "" {
		return sqerr.New("Invalid Name: Table names can not be blank")
	}

	// make sure there are more than one cols.
	if tab.NumCol(profile) < 1 {
		return sqerr.Newf("Create Table: table must have at least one column")
	}

	// add to _Catalog
	err := _Catalog.Lock(profile)
	if err != nil {
		return err
	}
	defer _Catalog.Unlock(profile)

	// Err if there is already a table with the same name
	tDef, err := _Catalog.FindTableDef(profile, tableName)
	if err != nil {
		return err
	}
	if tDef != nil {
		return sqerr.Newf("Invalid Name: Table %s already exists", tableName)
	}
	_Catalog.tables[tableName] = tab

	return nil
}

// DropTable - remove table from list of tables
//		protected by a mutex to be concurrency safe
func DropTable(profile *sqprofile.SQProfile, name string) error {

	name = strings.ToLower(name)
	// Err is name begins with _
	if isUnderScore(name) {
		return sqerr.Newf("Invalid Name: %s - Unable to drop system tables", name)
	}

	err := _Catalog.Lock(profile)
	if err != nil {
		return err
	}
	defer _Catalog.Unlock(profile)

	// Err if table does not exist
	tab, err := _Catalog.FindTableDef(profile, name)
	if err != nil {
		return err
	}
	if tab == nil {
		return sqerr.Newf("Invalid Name: Table %s does not exist", name)
	}
	// Make sure that no one else is changing the table
	err = tab.Lock(profile)
	if err != nil {
		return err
	}
	// Unlock when done to make sure the lock tracking is correct
	defer tab.Unlock(profile)
	// remove from _Catalog
	_Catalog.tables[strings.ToLower(name)] = nil

	//Clear out the values
	tab.rowm = nil
	tab.tableCols = nil
	tab.tableName = ""

	return nil
}

// newTableCatalog - Initialize a new TableCatalog
func newTableCatalog() *tableCatalog {
	return &tableCatalog{tables: make(map[string]*TableDef), SQMtx: sqmutex.NewSQMtx("TableCatalog: ")}
}

// CatalogTables returns a sorted list of tablenames
func CatalogTables(profile *sqprofile.SQProfile) ([]string, error) {
	err := _Catalog.RLock(profile)
	if err != nil {
		return nil, err
	}
	defer _Catalog.RUnlock(profile)
	var tNames []string

	for tab := range _Catalog.tables {
		if tab != "" {
			if _Catalog.tables[tab] != nil {
				tNames = append(tNames, tab)

			}
		}
	}
	sort.Strings(tNames)
	return tNames, nil

}

// CatalogAllTables returns a sorted list of tablenames including dropped tables
func CatalogAllTables(profile *sqprofile.SQProfile) ([]string, error) {
	err := _Catalog.RLock(profile)
	if err != nil {
		return nil, err
	}
	defer _Catalog.RUnlock(profile)
	tNames := make([]string, len(_Catalog.tables))
	i := 0
	for tab := range _Catalog.tables {
		tNames[i] = tab
		i++
	}
	sort.Strings(tNames)
	return tNames, nil

}

// LockAll write locks the tableCatalog and all tables in it
func (tl *tableCatalog) LockAll(profile *sqprofile.SQProfile) error {
	err := tl.Lock(profile)
	if err != nil {
		return err
	}
	for _, tab := range tl.tables {
		if tab != nil {
			err = tab.Lock(profile)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//UnlockAll write unlocks the tableCatalog and all tables in it
func (tl *tableCatalog) UnlockAll(profile *sqprofile.SQProfile) {
	for _, tab := range tl.tables {
		if tab != nil {
			tab.Unlock(profile)
		}
	}
	tl.Unlock(profile)
}

// isUnderScore - Checks to see if first char in string is an underscore
func isUnderScore(name string) bool {
	for _, c := range name {
		return c == '_'
	}
	return false
}

//UnlockCatalog releases write locks against the Catalog and all tables in it
func UnlockCatalog(profile *sqprofile.SQProfile) {
	_Catalog.UnlockAll(profile)
}

//LockCatalog reserves write locks on the Catalog and all tables in it
func LockCatalog(profile *sqprofile.SQProfile) {
	_Catalog.LockAll(profile)
}

//GetTable returns a table definition for the given table name
func GetTable(profile *sqprofile.SQProfile, tableName string) (*TableDef, error) {
	return _Catalog.FindTableDef(profile, tableName)
}
