package cc

import "github.com/llxisdsh/cc/internal/opt"

// FairSemaphore is a counting semaphore that guarantees FIFO (First-In-First-Out) order.
//
// Standard Semaphores (like [golang.org/x/sync/semaphore]) generally optimize for throughput
// and may allow barging (new waiters stealing permits), which can lead to starvation
// or unfairness in specific workloads.
//
// [FairSemaphore] ensures that permits are strictly assigned to waiters in the order of arrival.
//
// Implementation:
// It uses a mutex-protected linked list of waiters and a [TicketLock] for the mutex itself
// to ensure even the internal lock acquisition is fair.
//
// Limitations:
// - Max permits: 2^63 - 1. Panics on overflow.
type FairSemaphore struct {
	_       noCopy
	mu      TicketLock
	permits int64
	head    *fairWaiter
	tail    *fairWaiter
}

type fairWaiter struct {
	next *fairWaiter
	n    int64
	sema opt.Sema
}

// NewFairSemaphore creates a FairSemaphore with the given number of permits.
func NewFairSemaphore(permits int64) *FairSemaphore {
	return &FairSemaphore{permits: permits}
}

// Acquire blocks until n permits are available, then takes them.
func (s *FairSemaphore) Acquire(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	if s.head == nil && s.permits >= n {
		s.permits -= n
		s.mu.Unlock()
		return
	}
	w := &fairWaiter{n: n}
	if s.tail == nil {
		s.head = w
		s.tail = w
	} else {
		s.tail.next = w
		s.tail = w
	}
	s.mu.Unlock()
	w.sema.Acquire()
}

// TryAcquire attempts to acquire n permits without blocking.
func (s *FairSemaphore) TryAcquire(n int64) bool {
	if n <= 0 {
		return true
	}
	s.mu.Lock()
	if s.head != nil || s.permits < n {
		s.mu.Unlock()
		return false
	}
	s.permits -= n
	s.mu.Unlock()
	return true
}

// Release returns n permits and wakes waiting goroutines.
func (s *FairSemaphore) Release(n int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	if s.permits > 0 && s.permits+n < 0 {
		s.mu.Unlock()
		panicFairSemaphorePermitOverflow()
	}
	s.permits += n
	for s.head != nil && s.permits >= s.head.n {
		w := s.head
		s.permits -= w.n
		s.head = w.next
		if s.head == nil {
			s.tail = nil
		}
		w.sema.Release()
	}
	s.mu.Unlock()
}

//go:noinline
func panicFairSemaphorePermitOverflow() {
	panic("cc: FairSemaphore permit overflow")
}
