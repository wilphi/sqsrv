package sqmutex

import (
	"fmt"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
)

type lockData struct {
	TestName  string
	Profile   *sqprofile.SQProfile
	RW        *SQMutex
	Function  string
	ExpPanic  bool
	LockNames []string
	ExpVals   []int
}

func TestLocks(t *testing.T) {
	profile1 := sqprofile.CreateSQProfile()
	rw1 := NewSQMutex("T")

	data := []lockData{
		{TestName: "VerifyNoLocks empty map", Profile: profile1, Function: "VERIFY", ExpPanic: false, LockNames: nil, ExpVals: nil},
		{TestName: "WRITE lock", Profile: profile1, RW: &rw1, Function: "LOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 1}},
		{TestName: "READ lock after write", Profile: profile1, RW: &rw1, Function: "RLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 2}},
		{TestName: "VerifyNoLocks NON empty map", Profile: profile1, Function: "VERIFY", ExpPanic: true, LockNames: nil, ExpVals: nil},
		{TestName: "READ unlock", Profile: profile1, RW: &rw1, Function: "RUNLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 1}},
		{TestName: "WRITE unlock", Profile: profile1, RW: &rw1, Function: "UNLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 0}},
		{TestName: "VerifyNoLocks emptied map", Profile: profile1, Function: "VERIFY", ExpPanic: false, LockNames: nil, ExpVals: nil},
		{TestName: "READ unlock when no lock", Profile: profile1, RW: &rw1, Function: "RUNLOCK", ExpPanic: true, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 1}},
		{TestName: "WRITE unlock when no lock", Profile: profile1, RW: &rw1, Function: "UNLOCK", ExpPanic: true, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 0}},
		{TestName: "READ lock before write", Profile: profile1, RW: &rw1, Function: "RLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 1}},
		{TestName: "WRITE lock after READ", Profile: profile1, RW: &rw1, Function: "LOCK", ExpPanic: true, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 1}},
		{TestName: "UNLOCK last READ", Profile: profile1, RW: &rw1, Function: "RUNLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 0}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testLocksFunc(&row))

	}
}

func testLocksFunc(d *lockData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if d.ExpPanic && r == nil {
				t.Error(d.TestName + " did not panic")
			}
			if !d.ExpPanic && r != nil {
				t.Error(d.TestName + " panicked unexpectedly")
			}
		}()
		log.Warn(">>>" + d.TestName)

		switch d.Function {
		case "VERIFY":
			d.Profile.VerifyNoLocks()
		case "CHECK":
			for i, lname := range d.LockNames {
				retv := d.Profile.CheckLock(lname)
				if retv != d.ExpVals[i] {
					t.Errorf("Returned val: %d does not match expected val: %d for lock %s", retv, d.ExpVals[i], lname)
				}
			}
		case "LOCK":
			d.RW.Lock(d.Profile)
			for i, lname := range d.LockNames {
				if d.Profile.CheckLock(lname) != d.ExpVals[i] {
					t.Errorf("%s: Stored value %d does not match expected value %d", lname, d.Profile.CheckLock(lname), d.ExpVals[i])
				}
			}
		case "UNLOCK":
			d.RW.Unlock(d.Profile)
			for i, lname := range d.LockNames {
				if d.Profile.CheckLock(lname) != d.ExpVals[i] {
					t.Errorf("%s: Stored value %d does not match expected value %d", lname, d.Profile.CheckLock(lname), d.ExpVals[i])
				}
			}
		case "RLOCK":
			d.RW.RLock(d.Profile)
			for i, lname := range d.LockNames {
				if d.Profile.CheckLock(lname) != d.ExpVals[i] {
					t.Errorf("%s: Stored value %d does not match expected value %d", lname, d.Profile.CheckLock(lname), d.ExpVals[i])
				}
			}
		case "RUNLOCK":
			d.RW.RUnlock(d.Profile)
			for i, lname := range d.LockNames {
				if d.Profile.CheckLock(lname) != d.ExpVals[i] {
					t.Errorf("%s: Stored value %d does not match expected value %d", lname, d.Profile.CheckLock(lname), d.ExpVals[i])
				}
			}

		default:
			t.Errorf("Function is invalid: %q", d.Function)
		}

	}
}
