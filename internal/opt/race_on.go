//go:build race

package opt

import (
	"sync"
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
