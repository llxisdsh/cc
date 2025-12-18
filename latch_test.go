package cc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLatch_Boundary(t *testing.T) {
	l := &Latch{}
	// State = 0xFFFFFFFE (Max count, done=0).
	l.state.Store(0xFFFFFFFE)

	done := make(chan bool)
	go func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Latch.Wait should panic on overflow")
			} else if r != "cc: Latch waiter overflow" {
				t.Errorf("Unexpected panic: %v", r)
			}
			done <- true
		}()
		l.Wait()
	}()
	<-done
}

func TestLatchBasic(t *testing.T) {
	var e Latch

	start := time.Now()
	time.AfterFunc(100*time.Millisecond, func() {
		e.Open()
	})

	e.Wait()
	dur := time.Since(start)
	if dur < 100*time.Millisecond {
		t.Errorf("Wait returned too early: %v", dur)
	}
}

func TestLatchOpen(t *testing.T) {
	var e Latch
	var count int32
	var wg sync.WaitGroup
	n := 10

	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			e.Wait()
			atomic.AddInt32(&count, 1)
		}()
	}

	// Ensure they are waiting
	time.Sleep(50 * time.Millisecond)
	if c := atomic.LoadInt32(&count); c != 0 {
		t.Errorf("Waiters passed early: %d", c)
	}

	e.Open()
	wg.Wait()

	if c := atomic.LoadInt32(&count); c != int32(n) {
		t.Errorf("Not all waiters woke up: %d / %d", c, n)
	}
}

func TestLatchOpenBeforeWait(t *testing.T) {
	var e Latch
	e.Open() // Open the door

	// Should not block
	done := make(chan struct{})
	go func() {
		e.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Wait blocked even though Open was called before")
	}
}

func TestLatchDoubleOpen(t *testing.T) {
	var e Latch
	e.Open()
	e.Open() // Should be safe
	e.Wait() // Should pass
}
