package cc

import (
	"sync/atomic"

	"github.com/llxisdsh/cc/internal/opt"
)

// Latch is a synchronization primitive for "wait for completion" (One-Way Door).
// It supports multiple waiters.
// Once Open() is called, all current and future Wait() calls return immediately.
// It is 8 bytes in size (4 byte state + 4 byte semaphore).
type Latch struct {
	_ noCopy
	// state 32-bit:
	//   bit 0: done flag (1 = done)
	//   bits 1-31: waiter count
	state atomic.Uint32
	sema  opt.Sema
}

const (
	latchDoneFlag  = 1
	latchOneWaiter = 2 // 1 << 1
)

// Open opens the door.
// It wakes up all currently blocked waiters.
// Any future calls to Wait() will return immediately.
// Open() is idempotent (can be called multiple times).
func (e *Latch) Open() {
	var spins int
	for {
		s := e.state.Load()
		if s&latchDoneFlag != 0 {
			return
		}
		if e.state.CompareAndSwap(s, s|latchDoneFlag) {
			waiters := s >> 1
			for range waiters {
				e.sema.Release()
			}
			return
		}
		delay(&spins)
	}
}

// Wait blocks until Open is called.
// If Open has already been called, it returns immediately.
func (e *Latch) Wait() {
	s := e.state.Load()
	if s&latchDoneFlag != 0 {
		return
	}
	e.slowWait()
}

func (e *Latch) slowWait() {
	for {
		s := e.state.Load()
		if s&latchDoneFlag != 0 {
			return
		}

		if e.state.CompareAndSwap(s, s+latchOneWaiter) {
			e.sema.Acquire()
			return
		}
	}
}
