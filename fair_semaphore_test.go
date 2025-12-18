package cc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFairSemaphore_Boundary(t *testing.T) {
	s := NewFairSemaphore(0)
	// Manually set permits to MaxInt64
	s.permits = 1<<63 - 1

	defer func() {
		if r := recover(); r == nil {
			t.Error("FairSemaphore.Release should panic on overflow")
		} else if r != "cc: FairSemaphore permit overflow" {
			t.Errorf("Unexpected panic: %v", r)
		}
	}()

	s.Release(1)
}

func TestFairSemaphore_Basic(t *testing.T) {
	s := NewFairSemaphore(1)
	s.Acquire(1)
	if s.TryAcquire(1) {
		t.Fatalf("expected TryAcquire to fail when no permits")
	}
	s.Release(1)
	if !s.TryAcquire(1) {
		t.Fatalf("expected TryAcquire to succeed after release")
	}
}

func TestFairSemaphore_Concurrent(t *testing.T) {
	s := NewFairSemaphore(3)
	const n = 30
	var wg sync.WaitGroup
	wg.Add(n)
	var counter int64
	for range n {
		go func() {
			defer wg.Done()
			s.Acquire(1)
			atomic.AddInt64(&counter, 1)
			time.Sleep(time.Millisecond)
			s.Release(1)
		}()
	}
	wg.Wait()
	if counter != n {
		t.Fatalf("counter = %d, want %d", counter, n)
	}
}
