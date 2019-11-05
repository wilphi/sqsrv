package sqmutex

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wilphi/sqsrv/sqprofile"
)

type mtxlockData struct {
	TestName  string
	Profile   *sqprofile.SQProfile
	RW        *SQMtx
	Function  string
	ExpPanic  bool
	ErrTxt    string
	LockNames []string
	ExpVals   []int
}

func TestMtxStats(t *testing.T) {
	t.Run("Test Empty Stats", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		resetMtxStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
		str := GetMtxStats()
		if str != "Write Lock Stats:\n    No mtxStats at this time.\nRead Lock Stats:\n    No mtxStats at this time." {
			t.Errorf("Zeroed MtxStats did not display properly: \n%s", str)
			return
		}
	})

	t.Run("Test Only Lock Stats", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		resetMtxStats(time.Millisecond*3, 4*time.Millisecond, 2, 7*time.Millisecond, 0, 0, 0, 0, 1, 400*time.Millisecond)
		str := GetMtxStats()
		if str != "Write Lock Stats:\n    No mtxStats at this time.\nRead Lock Stats:\n    Min: 3ms\n    Max: 4ms\n    Average: 3.5ms\n    Total Locks: 2\n" {
			t.Errorf("MtxStats did not display properly: \n%s", str)
			return
		}
	})

	t.Run("Test Write Lock Stats", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		resetMtxStats(0, 0, 0, 0, time.Millisecond*6, 14*time.Millisecond, 2, 1, 0, 20*time.Millisecond)
		str := GetMtxStats()
		if str != "Write Lock Stats:\n    Min: 6ms\n    Max: 14ms\n    Average: 10ms\n    Total Locks: 2\n\nRead Lock Stats:\n    No mtxStats at this time." {
			t.Errorf("MtxStats did not display properly: \n%s", str)
			return
		}
	})

	t.Run("Test Read & Write Lock Stats", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		resetMtxStats(time.Millisecond*3, 4*time.Millisecond, 2, 7*time.Millisecond, time.Millisecond*6, 14*time.Millisecond, 2, 1, 3, 20*time.Millisecond)
		str := GetMtxStats()
		if str != "Write Lock Stats:\n    Min: 6ms\n    Max: 14ms\n    Average: 10ms\n    Total Locks: 2\n\nRead Lock Stats:\n    Min: 3ms\n    Max: 4ms\n    Average: 3.5ms\n    Total Locks: 2\n" {
			t.Errorf("MtxStats did not display properly: \n%s", str)
			return
		}
	})
}

func TestMtxLocks(t *testing.T) {
	profile1 := sqprofile.CreateSQProfile()
	rw1 := NewSQMtx("T")

	data := []mtxlockData{
		{TestName: "VerifyNoLocks empty map", Profile: profile1, Function: "VERIFY", ExpPanic: false, LockNames: nil, ExpVals: nil},
		{TestName: "WRITE lock", Profile: profile1, RW: rw1, Function: "LOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 1}},
		{TestName: "READ lock after write", Profile: profile1, RW: rw1, Function: "RLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 2}},
		{TestName: "VerifyNoLocks NON empty map", Profile: profile1, Function: "VERIFY", ExpPanic: true, LockNames: nil, ExpVals: nil},
		{TestName: "READ unlock", Profile: profile1, RW: rw1, Function: "RUNLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 1}},
		{TestName: "WRITE unlock", Profile: profile1, RW: rw1, Function: "UNLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 0}},
		{TestName: "VerifyNoLocks emptied map", Profile: profile1, Function: "VERIFY", ExpPanic: false, LockNames: nil, ExpVals: nil},
		{TestName: "READ unlock when no lock", Profile: profile1, RW: rw1, Function: "RUNLOCK", ExpPanic: true, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 1}},
		{TestName: "WRITE unlock when no lock", Profile: profile1, RW: rw1, Function: "UNLOCK", ExpPanic: true, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 0}},
		{TestName: "READ lock before write", Profile: profile1, RW: rw1, Function: "RLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 1}},
		{TestName: "WRITE lock after READ", Profile: profile1, RW: rw1, Function: "LOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{1, 1}, ErrTxt: "has a readlock, so trying for a writelock will deadlock process"},
		{TestName: "UNLOCK last READ", Profile: profile1, RW: rw1, Function: "RUNLOCK", ExpPanic: false, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 0}},
		{TestName: "UNLOCK last READ", Profile: profile1, RW: rw1, Function: "RUNLOCK", ExpPanic: true, LockNames: []string{"T-WRITE", "T-READ"}, ExpVals: []int{0, 0}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testmtxLocksFunc(&row))

	}
}

func testmtxLocksFunc(d *mtxlockData) func(*testing.T) {
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
			err := d.RW.Lock(d.Profile)
			if err != nil {
				if strings.Contains(err.Error(), d.ErrTxt) {
					return
				}
				t.Error("Unexpected error in test: ", err)
				return
			}
			if d.ErrTxt != "" {
				t.Error("Error expected in test")
				return
			}
			for i, lname := range d.LockNames {
				ck := d.Profile.CheckLock(lname)
				if ck != d.ExpVals[i] {
					t.Errorf("%s: Stored value %d does not match expected value %d", lname, ck, d.ExpVals[i])
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
			err := d.RW.RLock(d.Profile)
			if err != nil {
				if strings.Contains(err.Error(), d.ErrTxt) {
					return
				}
				t.Error("Unexpected error in test: ", err)
				return
			}
			if d.ErrTxt != "" {
				t.Error("Error expected in test")
				return
			}
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
			return
		}

	}
}

func TestAsyncMtxLocks(t *testing.T) {
	profile1 := sqprofile.CreateSQProfile()
	profile2 := sqprofile.CreateSQProfile()
	profile3 := sqprofile.CreateSQProfile()
	profile4 := sqprofile.CreateSQProfile()
	//rwB := NewSQMtx("B")
	t.Run("Block Write Lock", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		rwA := NewSQMtx("A")
		rwA.SetTimeout(10 * time.Millisecond)

		err := rwA.Lock(profile1)
		if err != nil {
			t.Error("Initial lock failed")
			return
		}

		err = rwA.Lock(profile2)
		if err == nil || !strings.Contains(err.Error(), "Write Lock failed due to timeout:") {
			t.Error("Second lock should fail with timeout")
			return
		}
		rwA.Unlock(profile1)
		profile1.VerifyNoLocks()
		profile2.VerifyNoLocks()
	})

	t.Run("Block & Release Write Lock", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		rwA := NewSQMtx("A")
		rwA.SetTimeout(10 * time.Millisecond)

		err := rwA.Lock(profile1)
		if err != nil {
			t.Error("Initial lock failed")
			return
		}
		rwA.Unlock(profile1)
		fmt.Println("Post unlock")
		profile1.VerifyNoLocks()

	})
	t.Run("Block & Release Write Lock2", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		rwA := NewSQMtx("A")
		rwA.SetTimeout(10 * time.Millisecond)

		err := rwA.Lock(profile1)
		if err != nil {
			t.Error("Initial lock failed")
			return
		}
		go func() {
			fmt.Println("\nSo sleepy...")
			time.Sleep(5 * time.Millisecond)
			fmt.Println("\nI'm awake...")
			rwA.Unlock(profile1)
			fmt.Println("Post unlock")
			return
		}()
		runtime.Gosched()
		err = rwA.Lock(profile2)
		if err != nil {
			t.Error("Second lock should not fail: ", err)
			return
		}
		profile1.VerifyNoLocks()
		rwA.Unlock(profile2)
		profile2.VerifyNoLocks()
	})
	t.Run("Double Block with timeout", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		var wg sync.WaitGroup
		wg.Add(1)
		rwA := NewSQMtx("A")
		rwA.SetTimeout(5 * time.Millisecond)
		rwB := NewSQMtx("B")
		rwB.SetTimeout(10 * time.Millisecond)
		err := rwA.Lock(profile1)
		if err != nil {
			t.Error("Initial A lock failed")
			return
		}
		err = rwB.Lock(profile2)
		if err != nil {
			t.Error("Initial B lock failed")
			return
		}

		go func() {
			time.Sleep(10 * time.Millisecond)
			err = rwB.Lock(profile1)
			if err != nil {
				t.Error(err)
			}
			rwB.Unlock(profile1)
			rwA.Unlock(profile1)
			profile1.VerifyNoLocks()
			wg.Done()
			return
		}()
		err = rwA.Lock(profile2)
		if err == nil || !strings.Contains(err.Error(), "Write Lock failed due to timeout:") {
			t.Errorf("Profile %d: Second lock should fail with timeout", profile2.GetID())
			return
		}

		rwB.Unlock(profile2)
		wg.Wait()
		profile2.VerifyNoLocks()
	})

	t.Run("Block Read with Write Lock ", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		rwA := NewSQMtx("BLOCKED-READ")
		rwA.SetTimeout(10 * time.Millisecond)

		err := rwA.Lock(profile1)
		if err != nil {
			t.Error("Initial lock failed")
			return
		}
		err = rwA.RLock(profile2)

		if err == nil {
			t.Error("Second lock did not return an error as expected")
			return
		}
		log.Info(err)
		if !strings.Contains(err.Error(), "Read Lock failed due to timeout:") {
			t.Error("Second lock should fail with timeout")
			return
		}
		rwA.Unlock(profile1)
		profile1.VerifyNoLocks()
		profile2.VerifyNoLocks()
	})

	t.Run("Block Write with Read Lock ", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		lockName := "BLOCK|WITH|READ"
		rwA := NewSQMtx(lockName)
		rwA.SetTimeout(10 * time.Millisecond)

		err := rwA.RLock(profile1)
		if err != nil {
			t.Error("Initial lock failed")
			return
		}
		err = rwA.Lock(profile2)

		if err == nil {
			t.Error("Second lock did not return an error as expected")
			return
		}

		if !strings.Contains(err.Error(), "Write Lock "+lockName+" failed due to timeout:") {
			t.Error("Second lock should fail with timeout")
			return
		}
		rwA.RUnlock(profile1)
		profile1.VerifyNoLocks()
		profile2.VerifyNoLocks()
	})

	t.Run("Block Write with 3 Read Locks ", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		status := new(int64)
		rwA := NewSQMtx("MultiReadLocks")
		rwA.SetTimeout(100 * time.Millisecond)

		err := rwA.RLock(profile1)
		if err != nil {
			t.Error("Initial lock failed")
			return
		}
		err = rwA.RLock(profile2)
		if err != nil {
			t.Error("Initial lock failed")
			return
		}
		err = rwA.RLock(profile3)
		if err != nil {
			t.Error("Initial lock failed")
			return
		}
		go func() {
			time.Sleep(10 * time.Millisecond)
			rwA.RUnlock(profile1)
			rwA.RUnlock(profile3)
			rwA.RUnlock(profile2)
			atomic.StoreInt64(status, 1)
			return
		}()
		err = rwA.Lock(profile4)

		if err != nil {
			t.Error("Write lock failed with error: ", err)
			return
		}
		if !atomic.CompareAndSwapInt64(status, 1, 1) {
			t.Error("Write lock did not respect read locks")
			return
		}
		rwA.Unlock(profile4)
		profile1.VerifyNoLocks()
		profile2.VerifyNoLocks()
		profile3.VerifyNoLocks()
		profile4.VerifyNoLocks()

	})

	t.Run("Random test with R/W locks", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf("%s panicked unexpectedly", t.Name())
			}
		}()
		rwA := NewSQMtx("RAND")
		rwA.SetTimeout(100 * time.Millisecond)
		var wg sync.WaitGroup
		numWrite := 5
		numRead := 10 * numWrite
		wg.Add(numRead + numWrite)
		go func() {
			for i := 0; i < numRead; i++ {
				if i%5 == 4 {
					time.Sleep(10 * time.Millisecond)
				}
				time.Sleep(time.Millisecond)
				num := i
				go func() {
					defer wg.Done()
					pf := sqprofile.CreateSQProfile()
					err := rwA.RLock(pf)
					if err != nil {
						t.Errorf("ReadLock #%d failed with error: %s", num, err.Error())
						return
					}
					log.Printf(">> Read Lock #%d\n", num)
					time.Sleep(5 * time.Millisecond)
					rwA.RUnlock(pf)
					log.Printf("<< Read Lock Released #%d\n", num)
				}()
			}

		}()
		go func() {
			for i := 0; i < numWrite; i++ {
				num := i
				time.Sleep(10 * time.Millisecond)
				go func() {
					defer wg.Done()
					pf := sqprofile.CreateSQProfile()
					err := rwA.Lock(pf)
					log.Printf("===========================\n:::>> Write Lock #%d\n", num)
					if err != nil {
						t.Errorf("WriteLock #%d failed with error: %s", num, err.Error())
						return
					}
					time.Sleep(10 * time.Millisecond)
					rwA.Unlock(pf)
					log.Printf("<<::: Write Lock #%d Released\n===========================\n", num)
				}()
			}

		}()

		wg.Wait()
	})

}

//Used to clear stats for testing
func resetMtxStats(minRLock time.Duration, maxRLock time.Duration, countRLock int, totalRLock time.Duration,
	minLock time.Duration, maxLock time.Duration, countLock int,
	failedLock int, failedRlock int, totalLock time.Duration) {
	mtxStats.Lock()
	defer mtxStats.Unlock()

	mtxStats.minRLock = minRLock
	mtxStats.maxRLock = maxRLock
	mtxStats.countRLock = countRLock
	mtxStats.totalRLock = totalRLock
	mtxStats.minLock = minLock
	mtxStats.maxLock = maxLock
	mtxStats.countLock = countLock
	mtxStats.failedLock = failedLock
	mtxStats.failedRlock = failedRlock
	mtxStats.totalLock = totalLock
}
