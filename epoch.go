package cc

import (
	"sync/atomic"

	"github.com/llxisdsh/cc/internal/opt"
)

// Epoch represents a monotonically increasing counter that supports "wait for target" semantics.
// It is effective for coordinating phases, versions, or milestones.
//
// Features:
//   - Add(n): Advances the epoch by n.
//   - WaitAtLeast(n): Blocks until the epoch reaches at least n.
//
// This primitive avoids the "Thundering Herd" problem common in condition variables by
// managing an ordered list of waiters and only waking those whose requirements are met.
//
// Example:
//
//	e := Epoch{}
//	go func() { e.WaitAtLeast(5); print("Reached 5!") }()
//	e.Add(5) // Wakes the waiter
type Epoch struct {
	_     noCopy
	state atomic.Uint64
	mu    TicketLock
	head  *epochWaiter
	tail  *epochWaiter
}

type epochWaiter struct {
	target uint32
	sema   opt.Sema
	// next is protected by Epoch.mu
	next *epochWaiter
}

// Current returns the current epoch value.
func (e *Epoch) Current() uint32 {
	return uint32(e.state.Load())
}

// Add advances the epoch by delta and wakes waiters whose targets are met.
func (e *Epoch) Add(delta uint32) uint32 {
	if delta == 0 {
		return e.Current()
	}

	// 1. Atomic increment (fast path for writers)
	// We return the NEW value.
	newVal := uint32(e.state.Add(uint64(delta)))

	// 2. Wake waiters (slow path)
	// Only acquire lock if we need to wake someone.
	// Optimistically checking if there are waiters is racy without a "waiter count" in atomic.
	// But getting the lock is fine, strictly better than Thundering Herd.
	// Note: We could keep a separate atomic "waiter count" to skip lock if 0.

	e.mu.Lock()

	// Walk the list of waiters
	// We have a singly linked list.
	// We need to remove nodes that are satisfied (target <= newVal).

	var prev *epochWaiter
	curr := e.head

	for curr != nil {
		if curr.target <= newVal {
			// Wake this waiter
			curr.sema.Release()

			// Remove from list
			if prev == nil {
				e.head = curr.next
			} else {
				prev.next = curr.next
			}
			if curr == e.tail {
				e.tail = prev
			}

			// Move to next, but `prev` stays same
			curr = curr.next
		} else {
			// Keep in list
			prev = curr
			curr = curr.next
		}
	}

	e.mu.Unlock()
	return newVal
}

// WaitAtLeast blocks until the epoch reaches at least the target value.
func (e *Epoch) WaitAtLeast(target uint32) {
	// 1. Fast path: check if condition already met
	if uint32(e.state.Load()) >= target {
		return
	}
	e.waitAtLeastSlow(target)
}

func (e *Epoch) waitAtLeastSlow(target uint32) {
	// 2. Slow path: enqueue
	e.mu.Lock()
	// Check again inside lock
	if uint32(e.state.Load()) >= target {
		e.mu.Unlock()
		return
	}

	w := &epochWaiter{target: target}
	if e.tail == nil {
		e.head = w
		e.tail = w
	} else {
		e.tail.next = w
		e.tail = w
	}
	e.mu.Unlock()

	// 3. Sleep
	w.sema.Acquire()
}
