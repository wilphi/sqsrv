package sqmutex

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
)

var log = logrus.New()

type mtxmsg struct {
	ID   int64
	Type string
}

// DefaultTimeout sets the global timeout duration for locks
var DefaultTimeout = 2 * time.Minute

type mtxchan chan mtxmsg

// SQMtx is a read/write mutex with timeout for the SQSRV database
type SQMtx struct {
	name     string
	lockchan mtxchan
	wName    string
	rName    string
	rlockNum *int64
	timeout  time.Duration
}

var mtxStats struct {
	minRLock    time.Duration
	maxRLock    time.Duration
	countRLock  int
	totalRLock  time.Duration
	minLock     time.Duration
	maxLock     time.Duration
	countLock   int
	failedLock  int
	failedRlock int
	totalLock   time.Duration
	sync.RWMutex
}

// GetMtxStats returns a string contatining min, max, average duration times for Read & Write locks and also a count of locks for each time
func GetMtxStats() string {
	var ret string
	ret = "Write Lock Stats:\n"

	mtxStats.RLock()
	defer mtxStats.RUnlock()

	if mtxStats.countLock <= 0 {
		ret += "    No mtxStats at this time."
	} else {
		ret += fmt.Sprintf("    Min: %v\n    Max: %v\n    Average: %v\n    Total Locks: %d\n", mtxStats.minLock, mtxStats.maxLock, mtxStats.totalLock/time.Duration(mtxStats.countLock), mtxStats.countLock)
	}
	ret += "\nRead Lock Stats:\n"
	if mtxStats.countRLock <= 0 {
		ret += "    No mtxStats at this time."
	} else {
		ret += fmt.Sprintf("    Min: %v\n    Max: %v\n    Average: %v\n    Total Locks: %d\n", mtxStats.minRLock, mtxStats.maxRLock, mtxStats.totalRLock/time.Duration(mtxStats.countRLock), mtxStats.countRLock)
	}
	return ret
}

func init() {
	// setup logging
	logFile, err := os.Create("locks.log")
	if err != nil {
		panic(err)
	}

	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	log.SetLevel(logrus.WarnLevel)

}

// Lock - Lock Write Mutex
func (m *SQMtx) Lock(profile *sqprofile.SQProfile) error {
	// If the profile already has a Write lock then we do not need to try again
	if profile.CheckLock(m.wName) == 0 {

		//if there is a read lock then we are going to be deadlocked
		if profile.CheckLock(m.rName) != 0 {
			log.Errorf("Process %d has a readlock, so trying for a writelock will deadlock process", profile.GetID())
			return sqerr.Newf("Process %d has a readlock, so trying for a writelock will deadlock process", profile.GetID())
		}
		log.Printf(">>> Profile %d initiating Write Lock: %s\n", profile.GetID(), m.name)

		start := time.Now()
		select {
		case m.lockchan <- mtxmsg{ID: profile.GetID()}:
			for {
				if atomic.CompareAndSwapInt64(m.rlockNum, 0, 0) {
					// No Read Locks
					break
				} else {
					//There are still read locks
					length := time.Since(start)
					if length > m.timeout {
						// waited too long for read locks to clear
						log.Warnf(">>>> Profile %d - Write Lock %s failed due to timeout: %v\n", profile.GetID(), m.name, length)
						return sqerr.Newf("Profile %d - Write Lock %s failed due to timeout: %v", profile.GetID(), m.name, length)
					}
					time.Sleep(time.Nanosecond)
				}
			}
		case <-time.After(m.timeout):
			length := time.Since(start)
			mtxStats.Lock()
			defer mtxStats.Unlock()
			mtxStats.failedLock++
			mtxStats.totalLock += length
			log.Warnf(">>>> Profile %d - %s Write Lock failed due to timeout: %v\n", profile.GetID(), m.name, length)
			return sqerr.Newf("Profile %d - %s Write Lock failed due to timeout: %v", profile.GetID(), m.name, length)

		}
		length := time.Since(start)
		mtxStats.Lock()
		if length < mtxStats.minLock || mtxStats.minLock == 0 {
			mtxStats.minLock = length
		}
		if length > mtxStats.maxLock {
			mtxStats.maxLock = length
		}
		mtxStats.totalLock += length

		mtxStats.countLock++
		mtxStats.Unlock()
		log.Printf(">>>> Profile %d - %s Write lock successful: %v\n", profile.GetID(), m.name, length)
	}
	profile.AddLock(m.wName, m.rName)
	return nil
}

// Unlock - Unlock Write Mutex
func (m *SQMtx) Unlock(profile *sqprofile.SQProfile) {
	// If the numlocks = 1 then unlock
	if profile.CheckLock(m.wName) == 1 {
		log.Printf("<<< Profile %d - %s completing Write Lock\n", profile.GetID(), m.name)
		<-m.lockchan
		log.Printf("<<< Profile %d - %s completed Write Lock\n", profile.GetID(), m.name)
	}
	profile.RemoveLock(m.wName, m.rName)
}

// RLock - Lock Read Mutex
func (m *SQMtx) RLock(profile *sqprofile.SQProfile) error {
	if profile.CheckLock(m.rName) == 0 {
		log.Printf("> Profile %d - %s initiating Read Lock\n", profile.GetID(), m.name)
		start := time.Now()
		select {
		case m.lockchan <- mtxmsg{ID: 1}:
			atomic.AddInt64(m.rlockNum, 1)
			<-m.lockchan
		case <-time.After(m.timeout):
			length := time.Since(start)
			mtxStats.Lock()
			mtxStats.failedRlock++
			mtxStats.totalRLock += length
			mtxStats.Unlock()
			log.Warnf(">>>> Profile %d - %s Read Lock failed due to timeout: %v\n", profile.GetID(), m.name, length)
			return sqerr.Newf("Profile %d - %s Read Lock failed due to timeout: %v", profile.GetID(), m.name, length)
		}

		length := time.Since(start)
		mtxStats.Lock()
		if length < mtxStats.minRLock || mtxStats.minRLock == 0 {
			mtxStats.minRLock = length
		}
		if length > mtxStats.maxRLock {
			mtxStats.maxRLock = length
		}
		mtxStats.totalRLock += length

		mtxStats.countRLock++
		mtxStats.Unlock()
		log.Printf(">>>> Profile %d - %s Read lock successful: %v\n", profile.GetID(), m.name, length)
	}
	profile.AddLock(m.rName)
	return nil
}

// RUnlock - Unlock Read Mutex
func (m *SQMtx) RUnlock(profile *sqprofile.SQProfile) {
	// If the numlocks = 1 then unlock
	if profile.CheckLock(m.rName) == 1 {
		nval := atomic.AddInt64(m.rlockNum, -1)

		log.Printf("<<< Profile %d - %s completed Read Lock, %d left", profile.GetID(), m.name, nval)
	}
	profile.RemoveLock(m.rName)

}

// SetTimeout sets how long it a Read or Write Lock operation will wait for a lock before timing out
//  The default is 2 minutes
func (m *SQMtx) SetTimeout(tOut time.Duration) {
	m.timeout = tOut
	log.Infof("Mtx %s timeout set to %s", m.name, m.timeout)
}

// NewSQMtx -
func NewSQMtx(name string) *SQMtx {
	c := make(mtxchan, 1)
	num := new(int64)
	mtx := SQMtx{name: name, wName: name + "-WRITE", rName: name + "-READ", rlockNum: num, lockchan: c, timeout: DefaultTimeout}
	return &mtx
}
