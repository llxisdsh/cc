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
