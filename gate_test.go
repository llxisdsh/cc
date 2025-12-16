package cc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGate_Simple(t *testing.T) {
	var e Gate

	// 1. Initially Close
	if e.IsOpen() {
		t.Error("expected unset")
	}

	// 2. Wait in bg
	done := make(chan bool)
	go func() {
		e.Wait()
		done <- true
	}()

	select {
	case <-done:
		t.Error("Wait returned too early")
	case <-time.After(10 * time.Millisecond):
		// OK
	}

	// 3. Open
	e.Open()
	if !e.IsOpen() {
		t.Error("expected Open")
	}

	<-done

	// 4. Wait again (should be immediate)
	start := time.Now()
	e.Wait()
	if time.Since(start) > time.Millisecond*100 {
		t.Error("Wait not immediate when Open")
	}

	// 5. Close
	e.Close()
	if e.IsOpen() {
		t.Error("expected Close after Close")
	}
}

func TestGate_Pulse(t *testing.T) {
	var g Gate
	var wg sync.WaitGroup

	// Start waiters
	const n = 10
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			g.Wait()
		}()
	}

	// Ensure they are waiting (best effort)
	time.Sleep(10 * time.Millisecond)

	// Pulse should wake them all
	g.Pulse()

	// Wait for them to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("waiters did not wake up after Pulse")
	}

	// Gate should be closed for new waiters
	if g.IsOpen() {
		t.Fatalf("gate should be closed after Pulse")
	}

	// New waiter should block
	blockCh := make(chan struct{})
	go func() {
		g.Wait()
		close(blockCh)
	}()

	select {
	case <-blockCh:
		t.Fatalf("new waiter should block after Pulse")
	case <-time.After(20 * time.Millisecond):
	}

	// Open to clean up
	g.Open()
	<-blockCh
}

func TestGate_Open(t *testing.T) {
	var e Gate
	var wg sync.WaitGroup
	const N = 10

	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			e.Wait()
		}()
	}

	time.Sleep(10 * time.Millisecond) // Give them time to block
	e.Open()
	wg.Wait() // Should all return
}

func TestGate_CloseRace(t *testing.T) {
	var e Gate
	const N = 1000
	var wg sync.WaitGroup

	// Reviewer Note:
	// This test rapidly Opens and Closes.
	// We want to ensure that if a thread successfully enters Wait(),
	// it eventually returns if Open happens.
	// But since Close happens too, it might get stuck?
	// No, Wait() only blocks if !Open.
	// If it blocks, it needs an Open to wake.
	// Our test driver will eventually keep it Open.

	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			e.Wait()
		}()
	}

	// Spam Open/Close
	for i := range 100 {
		e.Open()
		// yield to let some wake?
		if i%2 == 0 {
			time.Sleep(100 * time.Microsecond)
		}
		e.Close()
	}

	// Finally Open it
	e.Open()
	wg.Wait()
}

func TestGate_PingPong(t *testing.T) {
	// Simulate the example user gave (roughly)
	// toggle gate
	var e Gate
	var wg sync.WaitGroup

	// Consumers
	running := atomic.Bool{}
	running.Store(true)

	wg.Add(5)
	for range 5 {
		go func() {
			defer wg.Done()
			for running.Load() {
				e.Wait()
				// ... do work ...
				// yield to simulate work, otherwise we spin too fast on "Open" state
				if running.Load() {
					time.Sleep(time.Microsecond)
				}
			}
		}()
	}

	// Controller
	for range 5 {
		e.Open() // Open gate
		time.Sleep(time.Millisecond)
		e.Close() // Close gate
		time.Sleep(time.Millisecond)
	}

	// Finish
	e.Open()
	running.Store(false)
	wg.Wait()
}
