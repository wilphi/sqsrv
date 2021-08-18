package sqtables

import (
	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
)

//TableMap is a map of tabledef
type TableMap map[string]*TableDef

//STransaction holds all of the information about the current transaction
type STransaction struct {
	profile  *sqprofile.SQProfile
	TData    TableMap
	WLocks   TableMap
	RLocks   TableMap
	auto     bool
	complete bool
}

// Transaction is the interface for transactions
type Transaction interface {
	Auto() bool
	Commit() error
	TestCommit() error
	Rollback()
	CommitIfAuto() error
	RollbackIfAuto()
	AddRow(tab *TableDef, row RowInterface) error
	Delete(tab *TableDef, row RowInterface) error
	UpdateRow(tab *TableDef, row RowInterface) error
	AddLock(tab *TableDef) error
	Profile() *sqprofile.SQProfile
}

//TransData stores rows that have changed durring a transaction
type TransData map[sqptr.SQPtr]RowInterface

////////////////////////////////////

// BeginTrans starts a transaction
func BeginTrans(profile *sqprofile.SQProfile, auto bool) Transaction {
	trans := STransaction{profile: profile, TData: make(TableMap), WLocks: make(TableMap), RLocks: make(TableMap), auto: auto}

	return &trans
}

// Profile returns the profile in use for the transaction
func (t *STransaction) Profile() *sqprofile.SQProfile {
	return t.profile
}

// Auto returns true if this is an automatic transaction (ie not started by a BEGIN statment)
func (t *STransaction) Auto() bool {
	return t.auto
}

// Commit Transaction
func (t *STransaction) Commit() error {
	if t.complete {
		if !t.auto {
			return sqerr.NewInternal("Transaction is already complete")
		}
		return nil
	}
	for tname, transTab := range t.TData {
		tab, err := GetTable(t.profile, tname)
		if err != nil {
			return err
		}
		// commit rows
		for ptr, row := range transTab.rowm {
			//			if _, ok := tab.rowm[ptr]; ok {
			//				return sqerr.NewInternalf("Duplicate row on commit in table %s - ptr: %d", tab.GetName(t.profile), int(ptr))
			//			}
			rw := row.(*RowDef)
			rw.Table = tab
			tab.rowm[ptr] = rw
		}
	}
	t.TData = nil
	t.releaseAllLocks()
	t.complete = true
	return nil
}

// TestCommit Transaction
func (t *STransaction) TestCommit() error {

	return sqerr.New("TestCommit not implemented")
}

// Rollback Transaction
func (t *STransaction) Rollback() {
	if t.complete {
		if !t.auto {
			log.Panic("Double Rollback")
		}
		return
	}
	// Dump Data
	t.TData = nil

	// Release Locks
	t.releaseAllLocks()
	t.complete = true
}

// CommitIfAuto will commit the transaction if it is an automatic transaction
func (t *STransaction) CommitIfAuto() error {
	if t.Auto() {
		return t.Commit()
	}
	return nil
}

// RollbackIfAuto will rollback the transaction if it is an automatic transaction
func (t *STransaction) RollbackIfAuto() {
	if t.Auto() {
		t.Rollback()
	}
}

// AddRow to transaction
func (t *STransaction) AddRow(tab *TableDef, row RowInterface) error {
	if t.complete {
		return sqerr.NewInternal("Transaction is already complete")
	}

	tableName := tab.GetName(t.profile)
	transTab, ok := t.TData[tableName]
	if !ok {
		transTab = CreateTmpTableDef(t.profile, tab)
		t.TData[tableName] = transTab
	}
	rw := row.(*RowDef)
	rw.Table = transTab
	t.TData[tableName].rowm[row.GetPtr(t.profile)] = rw
	return nil
}

// Delete soft deletes a row from the given table in a transaction
func (t *STransaction) Delete(tab *TableDef, row RowInterface) error {
	if t.complete {
		return sqerr.NewInternal("Transaction is already complete")
	}

	tableName := tab.GetName(t.profile)
	transTab, ok := t.TData[tableName]
	if !ok {
		//t.Data[tableName] = make(map[sqptr.SQPtr]RowInterface)
		transTab = CreateTmpTableDef(t.profile, tab)
		t.TData[tableName] = transTab
	}
	ptr := row.GetPtr(t.profile)
	rw, ok := transTab.rowm[ptr]
	if !ok {
		rw = row
	}
	rowD := rw.(*RowDef)
	rowD.Table = transTab
	rowD.Delete(t.profile)
	t.TData[tableName].rowm[ptr] = rowD
	return nil
}

// UpdateRow to transaction
func (t *STransaction) UpdateRow(tab *TableDef, row RowInterface) error {
	if t.complete {
		return sqerr.NewInternal("Transaction is already complete")
	}

	tableName := tab.GetName(t.profile)
	transTab, ok := t.TData[tableName]
	if !ok {
		transTab = CreateTmpTableDef(t.profile, tab)
		t.TData[tableName] = transTab
	}
	rw := row.(*RowDef)
	rw.Table = transTab
	t.TData[tableName].rowm[row.GetPtr(t.profile)] = rw
	return nil
}

// AddLock adds a Write lock to the given table
func (t *STransaction) AddLock(tab *TableDef) error {
	if t.complete {
		return sqerr.NewInternal("Transaction is already complete")
	}

	err := tab.Lock(t.profile)
	if err != nil {
		return err
	}
	t.WLocks[tab.tableName] = tab
	return nil
}

/*
// AddRLock adds a Read lock to the given table
func (t *STransaction) AddRLock(tab *TableDef) error {
	err := tab.RLock(t.profile)
	if err != nil {
		return err
	}
	t.RLocks[tab.tableName] = tab
	return nil
}
*/

// releaseAllLocks unlocks all locks for the transaction
func (t *STransaction) releaseAllLocks() {

	/*
		for tableName, tab := range t.RLocks {
			n := t.profile.CheckLock("Table: " + tableName + "-READ")
			for i := 0; i < n; i++ {
				tab.RUnlock(t.profile)
			}

		}
	*/

	for tableName, tab := range t.WLocks {
		n := t.profile.CheckLock("Table: " + tableName + "-WRITE")
		for i := 0; i < n; i++ {
			tab.Unlock(t.profile)
		}

	}
	t.profile.VerifyNoLocks()
}
