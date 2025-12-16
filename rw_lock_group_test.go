package cc

import (
	"sync"
	"testing"
	"time"
)

func TestRWLockGroup_Basic(t *testing.T) {
	var g RWLockGroup[string]
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)

	// Test Concurrent Readers
	for range n {
		go func() {
			defer wg.Done()
			g.RLock("key")
			time.Sleep(time.Microsecond)
			g.RUnlock("key")
		}()
	}
	wg.Wait()

	// Test Writer Exclusion
	g.Lock("key")
	done := make(chan struct{})
	go func() {
		g.RLock("key") // Should block
		close(done)
		g.RUnlock("key")
	}()

	select {
	case <-done:
		t.Fatal("RLock acquired while Lock held")
	case <-time.After(10 * time.Millisecond):
	}
	g.Unlock("key")

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("RLock not acquired after Unlock")
	}
}

func TestRWLockGroup_RefCounting(t *testing.T) {
	var g RWLockGroup[int]

	// 1. RLock -> Ref=1
	g.RLock(1)
	if _, ok := g.m.Load(1); !ok {
		t.Fatal("Entry should exist after RLock")
	}

	// 2. Lock -> Ref=2
	// Note: Lock will block if we try to acquire it on same goroutine,
	// but here we just check ref count logic via another goroutine or carefully.
	// Actually Lock() waits for RLock. So we can't Lock() on same thread without deadlock if implemented correctly.
	// Let's just RUnlock and check cleanup.

	g.RUnlock(1)

	// 3. Ref=0 -> Deleted
	if _, ok := g.m.Load(1); ok {
		t.Fatal("Entry should be auto-deleted after RUnlock (ref=0)")
	}
}
