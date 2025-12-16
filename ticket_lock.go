package cc

import (
	"sync/atomic"
)

// TicketLock is a fair, FIFO (First-In-First-Out) spin-lock.
//
// Unlike sync.Mutex, which allows "barging" (newcomers can steal the lock),
// TicketLock guarantees that goroutines acquire the lock in the exact order they called Lock().
//
// Implementation:
// It uses the classic "ticket" algorithm.
//   - Lock(): Takes a ticket number. Spins/Sleeps until `serving` == `my_ticket`.
//   - Unlock(): Increments `serving`, allowing the next ticket holder to proceed.
//
// Trade-offs:
//   - Pros: Strict fairness, preventing starvation. ideal for latency-sensitive but
//     high-contention scenarios where tail latency matters.
//   - Cons: Under high contention with long critical sections, it can suffer from
//     "lock convoy" if a thread goes to sleep while holding the ticket.
//     However, this implementation uses a hybrid strategy (spin + adaptive delay)
//     to mitigate pure busy-wait issues.
//
// It is recommended for protecting very small critical sections (referencing a few fields)
// where fairness is strictly required.
type TicketLock struct {
	_       noCopy
	next    atomic.Uint32
	serving atomic.Uint32
}

// Lock acquires the lock. Blocks until the lock is available.
func (m *TicketLock) Lock() {
	my := m.next.Add(1) - 1
	var spins int
	for {
		if m.serving.Load() == my {
			return
		}
		delay(&spins)
	}
}

// Unlock releases the lock.
func (m *TicketLock) Unlock() {
	m.serving.Add(1)
}
