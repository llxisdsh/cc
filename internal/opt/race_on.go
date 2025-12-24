//go:build race

package opt

import (
	"sync"
	"sync/atomic"
)

const Race_ = true

// Sema is a race-detector friendly semaphore implementation.
// It uses sync.Mutex and sync.Cond to ensure visibility to the race detector.
type Sema struct {
	mu    sync.Mutex
	cond  *sync.Cond
	count int
}

func (s *Sema) Acquire() {
	s.mu.Lock()
	if s.cond == nil {
		s.cond = sync.NewCond(&s.mu)
	}
	for s.count <= 0 {
		s.cond.Wait()
	}
	s.count--
	s.mu.Unlock()
}

func (s *Sema) Release() {
	s.mu.Lock()
	if s.cond == nil {
		s.cond = sync.NewCond(&s.mu)
	}
	s.count++
	s.cond.Signal()
	s.mu.Unlock()
}

// PLocalSlotLock is a simple spinlock using atomic operations.
// We cannot use sync.Mutex because it may block (park) the goroutine,
// and we are holding a P (via runtime_procPin), so we cannot allow parking.
// This spinlock is sufficient for race detector annotations and short critical sections.
type PLocalSlotLock struct {
	state atomic.Int32
}

func (l *PLocalSlotLock) Lock() {
	for !l.state.CompareAndSwap(0, 1) {
		// Busy wait. We cannot use runtime.Gosched() safely here while pinned.
		// Since PLocal contention is only expected from ForEach (rare),
		// this should be acceptable.
	}
}

func (l *PLocalSlotLock) Unlock() {
	l.state.Store(0)
}
