package cc

import (
	"sync"
	"testing"
	"time"
)

// Helper to set private state for whitebox testing
func setEpochState(e *Epoch, val uint64) {
	e.state.Store(val)
}

func TestEpoch_Boundary(t *testing.T) {
	e := &Epoch{}
	// Set state to MaxUint32 to verify we can cross the old boundary
	setEpochState(e, 0xFFFFFFFF)

	e.Add(1)
	if e.Current() != 0x100000000 {
		t.Errorf("Epoch failed to cross 32-bit boundary. Got %x", e.Current())
	}

	// Test WaitAtLeast works across boundary
	target := uint64(0x100000000) + 1

	done := make(chan bool)
	go func() {
		e.WaitAtLeast(target)
		done <- true
	}()

	e.Add(1) // Now at target
	<-done
}

func TestEpoch_WaitAndAdd(t *testing.T) {
	var e Epoch
	done := make(chan struct{})
	go func() {
		e.WaitAtLeast(1)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	if e.Current() != 0 {
		t.Fatalf("unexpected current before Add: %d", e.Current())
	}
	e.Add(1)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("WaitAtLeast did not return after Add")
	}
	if e.Current() != 1 {
		t.Fatalf("current = %d, want 1", e.Current())
	}
}

func TestEpoch_MultipleWaiters(t *testing.T) {
	var e Epoch
	const waiters = 10
	var wg sync.WaitGroup
	wg.Add(waiters)
	for range waiters {
		go func() {
			defer wg.Done()
			e.WaitAtLeast(3)
		}()
	}
	time.Sleep(10 * time.Millisecond)
	if e.Current() != 0 {
		t.Fatalf("unexpected current before increments: %d", e.Current())
	}
	e.Add(1)
	e.Add(1)
	time.Sleep(10 * time.Millisecond)
	if e.Current() != 2 {
		t.Fatalf("current = %d, want 2", e.Current())
	}
	e.Add(1)
	wg.Wait()
	if e.Current() != 3 {
		t.Fatalf("current = %d, want 3", e.Current())
	}
}
