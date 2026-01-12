package cc

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	p := NewWorkerPool(2, 10)

	var cnt atomic.Int64
	total := 20

	for range total {
		err := p.Submit(func() {
			cnt.Add(1)
			time.Sleep(1 * time.Millisecond)
		})
		if err != nil {
			t.Errorf("Submit failed: %v", err)
		}
	}

	// Graceful shutdown check
	// Close should wait for all tasks to complete
	p.Close()

	if cnt.Load() != int64(total) {
		t.Errorf("expected %d, got %d", total, cnt.Load())
	}

	// Test Submit after Close
	err := p.Submit(func() {})
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
}

func TestWorkerPool_Panic(t *testing.T) {
	// Worker should survive or recover from panic
	p := NewWorkerPool(1, 10)
	defer p.Close()

	// 1. Submit a panic task
	_ = p.Submit(func() {
		panic("oops")
	})

	// 2. Submit a normal task
	done := make(chan struct{})
	_ = p.Submit(func() {
		close(done)
	})

	select {
	case <-done:
		// Passed
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Worker died or did not process subsequent task")
	}
}

func TestWorkerPool_Lazy(t *testing.T) {
	p := NewWorkerPool(10, 10)
	// Internal field check (using reflection or just implicit behavior)
	// Since we can't easily access private fields without reflection in external test,
	// we will assume it works if tests pass and we trust the code.
	// But since this is same package `cc`, we CAN access private fields!

	if p.workers.Load() != 0 {
		t.Errorf("expected 0 workers initially, got %d", p.workers.Load())
	}

	_ = p.Submit(func() { time.Sleep(10 * time.Millisecond) })

	// Should start 1 worker
	if p.workers.Load() != 1 {
		t.Errorf("expected 1 worker, got %d", p.workers.Load())
	}

	p.Close()
}

func TestWorkerPool_Race(t *testing.T) {
	p := NewWorkerPool(10, 1000)
	var closed atomic.Bool

	go func() {
		time.Sleep(10 * time.Millisecond)
		p.Close()
		closed.Store(true)
	}()

	for range 1000 {
		err := p.Submit(func() {})
		if err != nil {
			if !errors.Is(err, ErrPoolClosed) {
				t.Fatalf("unexpected error: %v", err)
			}
		}
		if closed.Load() && err == nil {
			// It is possible to submit right before close strictly happens,
			// but once closed is true, eventually we should see errors.
			// This check is loose.
			_ = 0
		}
	}
}

// =============================================================================
// Benchmarks: WorkerPool
// =============================================================================

func BenchmarkWorkerPool(b *testing.B) {
	// 100 workers, queue size 1000
	wp := NewWorkerPool(100, 1000)
	defer wp.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wg := sync.WaitGroup{}
			wg.Add(1)
			err := wp.Submit(func() {
				wg.Done()
			})
			if err != nil {
				// Should not happen in benchmark unless queue full and blocking?
				// Our submit blocks if full.
				b.Error(err)
			}
			wg.Wait()
		}
	})
}

func TestWorkerPool_Wait(t *testing.T) {
	p := NewWorkerPool(4, 100)
	defer p.Close()

	var cnt atomic.Int64
	for range 50 {
		_ = p.Submit(func() {
			time.Sleep(1 * time.Millisecond)
			cnt.Add(1)
		})
	}

	// Wait without closing
	p.Wait()

	if cnt.Load() != 50 {
		t.Errorf("expected 50, got %d", cnt.Load())
	}

	// Pool should still be usable
	_ = p.Submit(func() {
		cnt.Add(1)
	})
	// Give time for task to be picked up before checking Wait
	time.Sleep(5 * time.Millisecond)
	p.Wait()

	if cnt.Load() != 51 {
		t.Errorf("expected 51, got %d", cnt.Load())
	}
}

func TestWorkerPool_PendingRunning(t *testing.T) {
	p := NewWorkerPool(2, 100)
	defer p.Close()

	// Initially no pending/running
	if p.Pending() != 0 {
		t.Errorf("expected 0 pending, got %d", p.Pending())
	}

	// Submit tasks that block
	blocker := make(chan struct{})
	for range 10 {
		_ = p.Submit(func() {
			<-blocker
		})
	}

	// Give workers time to start
	time.Sleep(10 * time.Millisecond)

	// Should have 2 running (max workers) and 8 pending
	if p.Running() != 2 {
		t.Errorf("expected 2 running, got %d", p.Running())
	}
	if p.Pending() != 8 {
		t.Errorf("expected 8 pending, got %d", p.Pending())
	}

	close(blocker)
	p.Wait()
}
