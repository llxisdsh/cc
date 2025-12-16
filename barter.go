package cc

import (
	"sync/atomic"
	"unsafe"
)

// Barter (Exchanger) is a synchronization point where two goroutines swap values.
//
// The first goroutine arriving at the exchange point waits for the second.
// When the second arrives, they exchange values and continue.
//
// Types:
//   - T: The type of value being exchanged.
//
// Usage:
//
//	b := NewBarter[string]()
//	// G1
//	v := b.Exchange("from G1")
//	// G2
//	v := b.Exchange("from G2")
//
// Implementation:
// Uses a "Slot" pointer.
//   - nil: Empty.
//   - Non-nil: A waiter is present with their value.
type Barter[T any] struct {
	_    noCopy
	slot atomic.Pointer[barterItem[T]]
}

type barterItem[T any] struct {
	value T
	latch Latch // Using Latch as a one-shot signal (optimized standard primitive)
	match *T    // Pointer to store the matched value
}

// NewBarter creates a new Barter exchanger.
func NewBarter[T any]() *Barter[T] {
	return &Barter[T]{}
}

// Exchange waits for another goroutine to arrive, then swaps values.
// It returns the value provided by the other goroutine.
func (b *Barter[T]) Exchange(myValue T) T {
	me := &barterItem[T]{value: myValue}
	var spins int
	// Latch is zero-usable (waiters block until Open).

	// 1. Try to offer my item
	for {
		peer := b.slot.Load()
		if peer == nil {
			// Slot is empty. Try to occupy it.
			if b.slot.CompareAndSwap(nil, me) {
				// Success. Now wait for a match.
				me.latch.Wait()
				// My 'match' field has been filled by the peer.
				match := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&me.match)))
				return *(*T)(match)
			}
			// CAS failed, retry (someone else might have taken slot or filled it).
		} else {
			// Slot is occupied. I am the second arrival.
			// Try to take the slot (clearing it to nil).
			if b.slot.CompareAndSwap(peer, nil) {
				// Success. I matched with 'peer'.
				// Give them my value.
				atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&peer.match)), unsafe.Pointer(&myValue))
				// Wake them up.
				peer.latch.Open()
				// Return their value.
				return peer.value
			}
			// CAS failed, retry.
		}
		delay(&spins)
	}
}
