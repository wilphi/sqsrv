package redo

import "sync/atomic"

// State holds the state of an object
type State struct {
	// can be Stopped (0), Started(2), Paused(1)
	iState uint64
}

// IsStopped indicates if the state is Stopped
func (s *State) IsStopped() bool {
	return atomic.LoadUint64(&s.iState) == 0
}

/* Not currently used
// IsStarted indicates if the state is Started
func (s *State) IsStarted() bool {
	return atomic.LoadUint64(&s.iState) == 2
}

// IsPaused indicates if the state is Paused
func (s *State) IsPaused() bool {
	return atomic.LoadUint64(&s.iState) == 1
}
*/

// Start set the state to Started
func (s *State) Start() {
	atomic.StoreUint64(&s.iState, 2)
}

/* not currently used
// Pause sets the state to Paused
func (s *State) Pause() {
	atomic.StoreUint64(&s.iState, 1)
}
*/

// Stop sets the state to Stopped
func (s *State) Stop() {
	atomic.StoreUint64(&s.iState, 0)
}
