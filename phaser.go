package cc

import (
	"sync/atomic"
)

// Phaser is a reusable synchronization barrier, similar to [java.util.concurrent.Phaser].
// It supports dynamic registration of parties and synchronization in phases.
//
// Concepts:
//   - Phase: An integer generation number.
//   - Parties: Number of registered participants.
//   - Arrive: A party signals it reached the barrier.
//   - Await: A party waits for others to arrive.
//
// Key Differences from [Rally]:
//   - Dynamic: Parties can be added/removed (Register/Deregister) at any time.
//   - Split-Phase: Arrive() and AwaitAdvance() are separate, allowing "Arrive and Continue" patterns.
//
// Size: 24 bytes (state + TicketLock + Epoch).
//
// Limitations:
// - Max parties: 65535. Panics on overflow.
type Phaser struct {
	_ noCopy

	// state stores (phase << 32) | (parties << 16) | (arrived << 0)
	// Limiting parties to 16-bit (65535).
	state atomic.Uint64

	// mu protects the read-modify-write operations on state.
	// Since these are short non-blocking updates, a fair TicketLock is perfect.
	mu TicketLock

	// epoch manages the "Wait for Phase N" logic.
	// We advance the epoch when a phase completes.
	epoch Epoch
}

// NewPhaser creates a new Phaser with 0 parties.
func NewPhaser() *Phaser {
	p := &Phaser{}
	return p
}

// Register adds a new party to the [Phaser].
// Returns the current phase number.
func (p *Phaser) Register() int {
	p.mu.Lock()
	if (p.state.Load()>>16)&0xFFFF == 0xFFFF {
		p.mu.Unlock()
		panicPhaserPartiesOverflow()
	}
	p.state.Add(1 << 16) // Increment parties
	phase := int(p.state.Load() >> 32)
	p.mu.Unlock()
	return phase
}

// Arrive signals that the current party has reached the barrier.
// It returns the current phase number.
// It does NOT wait for others.
func (p *Phaser) Arrive() int {
	p.mu.Lock()

	s := p.state.Load()
	phase := int(s >> 32)
	parties := int((s >> 16) & 0xFFFF)
	arrived := int(s & 0xFFFF)

	arrived++

	if arrived == parties {
		// Phase complete!
		// Reset arrived to 0, increment phase
		p.state.Store(uint64(phase+1)<<32 | uint64(parties)<<16) // arrived=0

		// Signal all waiters that phase+1 is reached.
		// Waiters wait for 'WaitAtLeast(phase+1)'.
		// Epoch is currently at 'phase' (conceptually).
		// Wait, Epoch.Add(1).
		// If explicit phase number matters: Epoch.WaitAtLeast(phase + 1).
		// We should ensure Epoch value matches Phase.
		// Assuming we started both at 0.
		p.epoch.Add(1)
		p.mu.Unlock()
		return phase + 1
	}

	// Update arrived count
	p.state.Store(uint64(phase)<<32 | uint64(parties)<<16 | uint64(arrived))
	p.mu.Unlock()
	return phase
}

// AwaitAdvance waits for the phase to advance from the given 'phase'.
// If the current phase is already greater than 'phase', it returns immediately.
// Returns the new phase number.
func (p *Phaser) AwaitAdvance(phase int) int {
	// Wait until Epoch >= phase + 1
	target := uint64(phase + 1)
	p.epoch.WaitAtLeast(target)
	return int(p.epoch.Current())
}

// ArriveAndAwaitAdvance Is equivalent to Arrive() then AwaitAdvance().
func (p *Phaser) ArriveAndAwaitAdvance() int {
	p.mu.Lock()
	// Lock held.

	s := p.state.Load()
	phase := int(s >> 32)
	parties := int((s >> 16) & 0xFFFF)
	arrived := int(s & 0xFFFF)

	arrived++

	if arrived == parties {
		// Advance phase
		nextPhase := phase + 1
		p.state.Store(uint64(nextPhase)<<32 | uint64(parties)<<16)
		p.epoch.Add(1) // Wakes everyone up
		p.mu.Unlock()
		return nextPhase
	}

	// Wait
	p.state.Store(uint64(phase)<<32 | uint64(parties)<<16 | uint64(arrived))
	p.mu.Unlock()

	// Block
	target := uint64(phase + 1)
	p.epoch.WaitAtLeast(target)

	return int(p.epoch.Current())
}

// ArriveAndDeregister signals arrival and removes the party.
func (p *Phaser) ArriveAndDeregister() int {
	p.mu.Lock()

	s := p.state.Load()
	phase := int(s >> 32)
	parties := int((s >> 16) & 0xFFFF)
	arrived := int(s & 0xFFFF)

	// Deregister means one less party involved
	parties--

	if parties == 0 {
		// Empty [Phaser]. Advance phase to clean up/unblock any weird state?
		// Advancing ensures consistency if someone was waiting (though they shouldn't be if we are the last).
		p.state.Store(uint64(phase+1) << 32) // parties=0, arrived=0
		p.epoch.Add(1)
		p.mu.Unlock()
		return phase + 1
	}

	if arrived == parties {
		// We were the last one needed.
		p.state.Store(uint64(phase+1)<<32 | uint64(parties)<<16)
		p.epoch.Add(1)
		p.mu.Unlock()
		return phase + 1
	}

	// Just update parties
	p.state.Store(uint64(phase)<<32 | uint64(parties)<<16 | uint64(arrived))
	p.mu.Unlock()
	return phase
}

//go:noinline
func panicPhaserPartiesOverflow() {
	panic("cc: Phaser parties overflow")
}
