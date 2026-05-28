package cc

import (
	"sync/atomic"

	"github.com/llxisdsh/cc/internal/opt"
)

// Rally is a reusable synchronization primitive that allows a set of
// goroutines to wait for each other to reach a common barrier point.
//
// It is useful in programs involving a fixed-size party of goroutines that
// must occasionally wait for each other. The barrier is called "cyclic"
// because it can be reused after the waiting logic is released.
//
// It is zero-value usable.
//
// Size: 16 bytes (8 byte state + 2*4 byte sema).
//
// Limitations:
// - Max parties: 2^32 - 1. Panics on overflow.
type Rally struct {
	_ noCopy
	// state 64-bit:
	//   High 32: Generation
	//   Low 32: Current Waiter Count
	state atomic.Uint64

	// sema is a double-buffered semaphore to prevent "signal stealing"
	// between generations.
	// Generation N waits on sema[N%2].
	sema [2]opt.Sema
}

// Meet waits until 'parties' number of callers have called Meet on this barrier.
//
// panic if parties <= 0.
//
// If the current goroutine is the last to arrive, it wakes up all other
// waiting goroutines and resets the barrier for the next generation.
//
// Returns the arrival index (0 to parties-1), where parties-1 indicates
// the caller was the last to arrive (the one who tripped the barrier).
func (b *Rally) Meet(parties int) int {
	if parties <= 0 {
		panicRallyPartiesMustBePositive()
	}
	if uint64(parties) > (1<<32)-1 {
		panicRallyPartiesExceedsMaxUint32()
	}

	// Fast path for single party
	if parties == 1 {
		return 0
	}

	for {
		s := b.state.Load()
		gen := s >> 32
		count := uint32(s)

		if count == uint32(parties)-1 {
			// We are the last to arrive.
			// Reset count to 0 and increment generation.
			nextState := (gen + 1) << 32
			if b.state.CompareAndSwap(s, nextState) {
				// Wake up all waiters from THIS generation.
				// They are waiting on sema[gen%2].
				semaPtr := &b.sema[gen%2]
				for i := 0; i < int(count); i++ {
					semaPtr.Release()
				}
				return int(count)
			}
		} else if b.state.CompareAndSwap(s, s+1) {
			// We are not the last. Increment waiter count.
			// Block on the semaphore for THIS generation.
			b.sema[gen%2].Acquire()
			return int(count)
		}
	}
}

//go:noinline
func panicRallyPartiesMustBePositive() {
	panic("cc: parties must be positive")
}

//go:noinline
func panicRallyPartiesExceedsMaxUint32() {
	panic("cc: parties exceeds max uint32")
}
