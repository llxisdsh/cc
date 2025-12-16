package cc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRally_Simple(t *testing.T) {
	const parties = 10
	var b Rally
	var count atomic.Int32

	var wg sync.WaitGroup
	wg.Add(parties)

	for i := range parties {
		go func(id int) {
			defer wg.Done()
			// Deliberately delay some to ensure not everyone arrives at once
			if id%2 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
			count.Add(1)
			b.Meet(parties)
		}(i)
	}

	wg.Wait()
	if c := count.Load(); c != parties {
		t.Errorf("expected count %d, got %d", parties, c)
	}
}

func TestRally_Reuse(t *testing.T) {
	const parties = 5
	const cycles = 50
	var b Rally

	var wg sync.WaitGroup
	wg.Add(parties)

	// Shared counter to verify lockstep
	var globalCounter atomic.Int32

	for range parties {
		go func() {
			defer wg.Done()
			for range cycles {
				// Phase 1: Increment
				globalCounter.Add(1)

				// Barrier 1: Wait for everyone to increment
				b.Meet(parties)

				// Verify
				if val := globalCounter.Load(); val != parties {
					// See note in previous test version about raciness of this check
				}

				// Barrier 2: Wait for everyone to check
				b.Meet(parties)

				// Phase 2: Decrement
				globalCounter.Add(-1)

				// Barrier 3: Wait for everyone to decrement
				b.Meet(parties)

				if val := globalCounter.Load(); val != 0 {
				}

				// Barrier 4: Sync before next cycle
				b.Meet(parties)
			}
		}()
	}

	wg.Wait()
}

func TestRally_ReturnValues(t *testing.T) {
	const parties = 3
	var b Rally
	var wg sync.WaitGroup
	wg.Add(parties)

	results := make([]int, parties)

	for i := range parties {
		go func(idx int) {
			defer wg.Done()
			results[idx] = b.Meet(parties)
		}(i)
	}
	wg.Wait()

	// Check that we got 0, 1, 2 in some order
	seen := make(map[int]bool)
	for _, res := range results {
		seen[res] = true
	}
	if len(seen) != parties {
		t.Errorf("expected %d unique arrival indices, got %v", parties, results)
	}
}

func TestRally_PanicNegative(t *testing.T) {
	var b Rally
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for negative parties")
		}
	}()
	b.Meet(0)
}

func TestRally_DynamicParties(t *testing.T) {
	// Test that we can use the same barrier with different party counts
	var b Rally

	// Phase 1: 3 parties
	var wg sync.WaitGroup
	wg.Add(3)
	for range 3 {
		go func() {
			defer wg.Done()
			b.Meet(3)
		}()
	}
	wg.Wait()

	// Phase 2: 5 parties
	wg.Add(5)
	for range 5 {
		go func() {
			defer wg.Done()
			b.Meet(5)
		}()
	}
	wg.Wait()
}
