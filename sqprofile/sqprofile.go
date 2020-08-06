package sqprofile

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

var profileID = new(int64)

// SQProfile contains the necessary lock and security information required for all functions
type SQProfile struct {
	id    int64
	locks map[string]int
	mux   sync.Mutex
}

// CreateSQProfile initializes a new profile
func CreateSQProfile() *SQProfile {
	return &SQProfile{id: atomic.AddInt64(profileID, 1), locks: make(map[string]int)}
}

// GetID returns the profile ID number
func (p *SQProfile) GetID() int64 {
	p.mux.Lock()
	defer p.mux.Unlock()
	return p.id
}

// CheckLock returns the number of times the lockname has been locked
func (p *SQProfile) CheckLock(lck string) int {
	p.mux.Lock()
	defer p.mux.Unlock()
	return p.locks[lck]
}

// AddLock adds indicators to the locks map
func (p *SQProfile) AddLock(locks ...string) {
	p.mux.Lock()
	defer p.mux.Unlock()
	for _, lck := range locks {
		p.locks[lck]++
	}
}

//RemoveLock removes indicators from the locks map
func (p *SQProfile) RemoveLock(locks ...string) {
	p.mux.Lock()
	defer p.mux.Unlock()
	for _, lck := range locks {
		if p.locks[lck] <= 0 {
			// We are trying to unlock something that is unlocked
			log.Panicf("Profile %d - %s is not locked but we are tring to unlock it", p.id, lck)
		}
		p.locks[lck]--
	}
}

// VerifyNoLocks checks to make sure that the number of calls to lock are balanced with calls to unlock
//	This is for both Read & Write locks. The program will panic if this check fails
func (p *SQProfile) VerifyNoLocks() {
	p.mux.Lock()
	defer p.mux.Unlock()
	// sort the key list
	keylist := make([]string, 0, len(p.locks))
	for k := range p.locks {
		keylist = append(keylist, k)
	}
	sort.Strings(keylist)
	keys := ""
	for _, k := range keylist {
		v := p.locks[k]
		if v > 0 {
			keys += fmt.Sprintf("%s = %d::", k, v)
		}
	}
	if keys != "" {
		frame := getFrame(1)
		keys = keys[:len(keys)-2]
		log.Panicf("Profile %d - Mismatched locks are: (%s) at: %s,:%d %s", p.id, keys, stripPath(frame.File), frame.Line, frame.Function)
	}

	return
}

func stripPath(path string) string {
	i := strings.Index(path, "/sqsrv/")
	return path[i:]
}

// getFrame returns the stack frame of the function that called it (skipFrames = 0), By increasing the skip
//  we can get previous callers. skipFrames = 1 is probably the most useful
func getFrame(skipFrames int) runtime.Frame {
	// We need the frame at index skipFrames+2, since we never want runtime.Callers and getFrame
	targetFrameIndex := skipFrames + 2

	// Set size to targetFrameIndex+2 to ensure we have room for one more caller than we need
	programCounters := make([]uintptr, targetFrameIndex+2)
	n := runtime.Callers(0, programCounters)

	frame := runtime.Frame{Function: "unknown"}
	if n > 0 {
		frames := runtime.CallersFrames(programCounters[:n])
		for more, frameIndex := true, 0; more && frameIndex <= targetFrameIndex; frameIndex++ {
			var frameCandidate runtime.Frame
			frameCandidate, more = frames.Next()
			if frameIndex == targetFrameIndex {
				frame = frameCandidate
			}
		}
	}

	return frame
}
