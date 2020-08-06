package sqtables

import (
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
)

//TableMap is a map of tabledef
type TableMap map[string]*TableDef

//Transaction holds all of the information about the current transaction
type Transaction struct {
	Profile *sqprofile.SQProfile
	//Data    map[string]TransData
	TData  TableMap
	auto   bool
	WLocks TableMap
	RLocks TableMap
}

//TransData stores rows that have changed durring a transaction
type TransData map[sqptr.SQPtr]RowInterface

////////////////////////////////////

// BeginTrans starts a transaction
func BeginTrans(profile *sqprofile.SQProfile, auto bool) *Transaction {
	//trans := Transaction{Profile: profile, Data: make(map[string]TransData), WLocks: make(TableMap), RLocks: make(TableMap)}
	trans := Transaction{Profile: profile, TData: make(TableMap), WLocks: make(TableMap), RLocks: make(TableMap), auto: auto}
	return &trans
}

// Auto returns true if this is an automatic transaction (ie not started by a BEGIN statment)
func (t *Transaction) Auto() bool {
	return t.auto
}

// Commit Transaction
func (t *Transaction) Commit() error {
	for tname, transTab := range t.TData {
		tab, err := GetTable(t.Profile, tname)
		if err != nil {
			return err
		}
		// commit rows
		for ptr, row := range transTab.rowm {
			if _, ok := tab.rowm[ptr]; ok {
				return sqerr.NewInternalf("Duplicate row on commit in table %s - ptr: %d", tab.GetName(t.Profile), int(ptr))
			}
			rw := row.(*RowDef)
			rw.Table = tab
			tab.rowm[ptr] = rw
		}
	}
	t.TData = nil
	t.releaseAllLocks()
	return nil
}

// TestCommit Transaction
func (t *Transaction) TestCommit() error {

	return sqerr.New("TestCommit not implemented")
}

// Rollback Transaction
func (t *Transaction) Rollback() {
	// Dump Data
	t.TData = nil

	// Release Locks
	t.releaseAllLocks()
}

// AutoComplete transaction
func (t *Transaction) AutoComplete() error {
	if !t.auto {
		return nil
	}

	err := t.Commit()
	if err != nil {
		t.Rollback()
		return err
	}
	return nil
}

// AddRow to transaction
func (t *Transaction) AddRow(tab *TableDef, row RowInterface) error {
	tableName := tab.GetName(t.Profile)
	transTab, ok := t.TData[tableName]
	if !ok {
		//t.Data[tableName] = make(map[sqptr.SQPtr]RowInterface)
		transTab = CreateTmpTableDef(t.Profile, tab)
		t.TData[tableName] = transTab
	}
	rw := row.(*RowDef)
	rw.Table = transTab
	t.TData[tableName].rowm[row.GetPtr(t.Profile)] = rw
	return nil
}

// AddLock adds a Write lock to the given table
func (t *Transaction) AddLock(tab *TableDef) error {
	err := tab.Lock(t.Profile)
	if err != nil {
		return err
	}
	t.WLocks[tab.tableName] = tab
	return nil
}

/*
// AddRLock adds a Read lock to the given table
func (t *Transaction) AddRLock(tab *TableDef) error {
	err := tab.RLock(t.Profile)
	if err != nil {
		return err
	}
	t.RLocks[tab.tableName] = tab
	return nil
}
*/

// releaseAllLocks unlocks all locks for the transaction
func (t *Transaction) releaseAllLocks() {

	/*
		for tableName, tab := range t.RLocks {
			n := t.Profile.CheckLock("Table: " + tableName + "-READ")
			for i := 0; i < n; i++ {
				tab.RUnlock(t.Profile)
			}

		}
	*/

	for tableName, tab := range t.WLocks {
		n := t.Profile.CheckLock("Table: " + tableName + "-WRITE")
		for i := 0; i < n; i++ {
			tab.Unlock(t.Profile)
		}

	}
	t.Profile.VerifyNoLocks()
}
