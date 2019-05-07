package sqprofile

import (
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestGetID(t *testing.T) {
	oldID := *profileID
	profile := CreateSQProfile()

	if profile.GetID() != oldID+1 {
		t.Error("GetID returned unexpected results")
	}

	if profile.id != profile.GetID() {
		t.Error("GetID returned unexpected results")

	}
}

type lockData struct {
	TestName  string
	Profile   *SQProfile
	Function  string
	ExpPanic  bool
	LockNames []string
	ExpVals   []int
}

func TestLocks(t *testing.T) {
	profile1 := CreateSQProfile()

	data := []lockData{
		{TestName: "VerifyNoLocks empty map", Profile: profile1, Function: "VERIFY", ExpPanic: false, LockNames: nil, ExpVals: nil},
		{TestName: "Add locks", Profile: profile1, Function: "ADD", ExpPanic: false, LockNames: []string{"TEST1-READ", "TEST2-WRITE"}, ExpVals: []int{1, 1}},
		{TestName: "Check locks", Profile: profile1, Function: "CHECK", ExpPanic: false, LockNames: []string{"TEST1-READ", "TEST2-WRITE"}, ExpVals: []int{1, 1}},
		{TestName: "Add other locks", Profile: profile1, Function: "ADD", ExpPanic: false, LockNames: []string{"TEST1-READ", "TEST3-READ"}, ExpVals: []int{2, 1}},
		{TestName: "Check other locks", Profile: profile1, Function: "CHECK", ExpPanic: false, LockNames: []string{"TEST1-READ", "TEST3-READ"}, ExpVals: []int{2, 1}},
		{TestName: "VerifyNoLocks NON empty map", Profile: profile1, Function: "VERIFY", ExpPanic: true, LockNames: nil, ExpVals: nil},
		{TestName: "Remove other locks", Profile: profile1, Function: "REMOVE", ExpPanic: false, LockNames: []string{"TEST1-READ", "TEST3-READ"}, ExpVals: []int{1, 0}},
		{TestName: "Remove locks", Profile: profile1, Function: "REMOVE", ExpPanic: false, LockNames: []string{"TEST1-READ", "TEST2-WRITE"}, ExpVals: []int{0, 0}},
		{TestName: "VerifyNoLocks emptied map", Profile: profile1, Function: "VERIFY", ExpPanic: false, LockNames: nil, ExpVals: nil},
		{TestName: "Remove when there is nothing to remove", Profile: profile1, Function: "REMOVE", ExpPanic: true, LockNames: []string{"TEST1-READ", "TEST2-WRITE"}, ExpVals: []int{0, 0}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testLocksFunc(row))

	}
}

func testLocksFunc(d lockData) func(*testing.T) {
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
		case "ADD":
			d.Profile.AddLock(d.LockNames...)
			for i, lname := range d.LockNames {
				if d.Profile.locks[lname] != d.ExpVals[i] {
					t.Errorf("Stored value %d does not match expected value %d", d.Profile.locks[lname], d.ExpVals[i])
				}
			}
		case "REMOVE":
			d.Profile.RemoveLock(d.LockNames...)
			for i, lname := range d.LockNames {
				if d.Profile.locks[lname] != d.ExpVals[i] {
					t.Errorf("Stored value %d does not match expected value %d", d.Profile.locks[lname], d.ExpVals[i])
				}
			}
		default:
			t.Errorf("Function is invalid: %q", d.Function)
		}

	}
}
