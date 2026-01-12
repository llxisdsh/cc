package cc

import (
	"sync"
	"testing"
)

func TestTicketLock(t *testing.T) {
	var m TicketLock
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	var counter int64
	for range n {
		go func() {
			defer wg.Done()
			m.Lock()
			counter++
			m.Unlock()
		}()
	}
	wg.Wait()
	if counter != n {
		t.Fatalf("counter = %d, want %d", counter, n)
	}
}

func TestTicketLock_TryLock(t *testing.T) {
	var m TicketLock

	// TryLock should succeed on free lock
	if !m.TryLock() {
		t.Error("TryLock should succeed on free lock")
	}
	m.Unlock()

	// TryLock should fail when locked
	m.Lock()
	if m.TryLock() {
		t.Error("TryLock should fail when locked")
	}
	m.Unlock()
}
