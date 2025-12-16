package opt

import (
	"sync"
	"testing"
	"time"
)

func TestSemaWrapper(t *testing.T) {
	var s Sema

	// 1. Basic block/unblock
	done := make(chan struct{})
	go func() {
		s.Acquire()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Acquire returned before Release")
	case <-time.After(50 * time.Millisecond):
		// OK
	}

	s.Release()
	select {
	case <-done:
		// OK
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Acquire did not return after Release")
	}

	// 2. Multiple waiters
	var wg sync.WaitGroup
	n := 10
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			s.Acquire()
		}()
	}

	// Give them time to block
	time.Sleep(50 * time.Millisecond)

	// Wake them up one by one
	for range n {
		s.Release()
	}

	// Wait for all
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case <-ch:
		// OK
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Not all waiters woke up")
	}
}
