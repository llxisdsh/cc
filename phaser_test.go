package cc

import (
	"testing"
)

func TestPhaser_Basic(t *testing.T) {
	p := NewPhaser()
	p.Register() // 1 party (Main)

	// Start a worker
	p.Register() // 2 parties
	go func() {
		// Phase 0
		p.ArriveAndAwaitAdvance()
		// Phase 1
		p.ArriveAndDeregister()
	}()

	// Phase 0
	phase := p.ArriveAndAwaitAdvance()
	if phase != 1 {
		t.Errorf("expected phase 1, got %d", phase)
	}

	// Phase 1: worker deregisters.
	// Only 1 party left (me).
	// Calling Arrive should advance immediately.
	phase = p.ArriveAndAwaitAdvance()
	if phase != 2 {
		t.Errorf("expected phase 2, got %d", phase)
	}
}

func TestPhaser_Dynamic(t *testing.T) {
	p := NewPhaser()
	p.Register() // Me

	const n = 5
	for range n {
		p.Register()
		go func() {
			p.ArriveAndAwaitAdvance()
		}()
	}

	// Wait for all to arrive
	p.ArriveAndAwaitAdvance()
	// All should pass
}
