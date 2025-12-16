package cc

import (
	"sync"
	"testing"
)

func TestTicketLockGroupBasic(t *testing.T) {
	var g TicketLockGroup[string]
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	counter := 0
	for range n {
		go func() {
			defer wg.Done()
			g.Lock("k")
			counter++
			g.Unlock("k")
		}()
	}
	wg.Wait()
	if counter != n {
		t.Fatalf("counter = %d, want %d", counter, n)
	}
}
