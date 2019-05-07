package sqmutex

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqprofile"
)

var log = logrus.New()

// SQMutex -
type SQMutex struct {
	name  string
	rw    sync.RWMutex
	wName string
	rName string
}

var stats struct {
	minRLock   time.Duration
	maxRLock   time.Duration
	countRLock int
	totalRLock time.Duration
	minLock    time.Duration
	maxLock    time.Duration
	countLock  int
	totalLock  time.Duration
}

// GetStats returns a string contatining min, max, average duration times for Read & Write locks and also a count of locks for each time
func GetStats() string {
	var ret string
	ret = "Write Lock Stats:\n"

	if stats.countLock <= 0 {
		ret += "    No stats at this time."
	} else {
		ret += fmt.Sprintf("    Min: %v\n    Max: %v\n    Average: %v\n    Total Locks: %d\n", stats.minLock, stats.maxLock, stats.totalLock/time.Duration(stats.countLock), stats.countLock)
	}
	ret += "\nRead Lock Stats:\n"
	if stats.countRLock <= 0 {
		ret += "    No stats at this time."
	} else {
		ret += fmt.Sprintf("    Min: %v\n    Max: %v\n    Average: %v\n    Total Locks: %d\n", stats.minRLock, stats.maxRLock, stats.totalRLock/time.Duration(stats.countRLock), stats.countRLock)
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
func (m *SQMutex) Lock(profile *sqprofile.SQProfile) {

	// If the profile already has a Write lock then we do not need to try again
	if profile.CheckLock(m.wName) == 0 {

		//if there is a read lock then we are going to be deadlocked
		if profile.CheckLock(m.rName) != 0 {
			log.Panicf("Process %d has a readlock, so trying for a writelock will deadlock process", profile.GetID())
		}
		log.Printf(">>> Profile %d initiating Write Lock: %s", profile.GetID(), m.name)
		start := time.Now()
		m.rw.Lock()
		length := time.Since(start)
		if length < stats.minLock || stats.minLock == 0 {
			stats.minLock = length
		}
		if length > stats.maxLock {
			stats.maxLock = length
		}
		stats.countLock++
		stats.totalLock += length

		log.Printf(">>>> %s Write lock successful: %v", m.name, length)
	}
	profile.AddLock(m.wName, m.rName)

}

// Unlock - Unlock Write Mutex
func (m *SQMutex) Unlock(profile *sqprofile.SQProfile) {
	// If the numlocks = 1 then unlock
	if profile.CheckLock(m.wName) == 1 {
		log.Printf("<<< %s completing Write Lock", m.name)
		m.rw.Unlock()
		log.Printf("<<< %s completed Write Lock", m.name)
	}
	profile.RemoveLock(m.wName, m.rName)
}

// RLock - Lock Read Mutex
func (m *SQMutex) RLock(profile *sqprofile.SQProfile) {
	if profile.CheckLock(m.rName) == 0 {
		log.Printf("> %s initiating Read Lock", m.name)
		start := time.Now()
		m.rw.RLock()
		length := time.Since(start)
		if length < stats.minRLock || stats.minRLock == 0 {
			stats.minRLock = length
		}
		if length > stats.maxRLock {
			stats.maxRLock = length
		}
		stats.countRLock++
		stats.totalRLock += length
		log.Printf(">>>> %s Read lock successful: %v", m.name, length)
	}
	profile.AddLock(m.rName)
}

// RUnlock - Unlock Read Mutex
func (m *SQMutex) RUnlock(profile *sqprofile.SQProfile) {
	// If the numlocks = 1 then unlock
	if profile.CheckLock(m.rName) == 1 {
		log.Printf("<<< %s completing Read Lock", m.name)
		m.rw.RUnlock()
		log.Printf("<<< %s completed Read Lock", m.name)
	}
	profile.RemoveLock(m.rName)

}

// NewSQMutex -
func NewSQMutex(name string) SQMutex {
	return SQMutex{name: name, wName: name + "-WRITE", rName: name + "-READ"}
}
