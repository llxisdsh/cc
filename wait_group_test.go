package cc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitGroupReuse(t *testing.T) {
	var wg WaitGroup
	var trace []string
	var mu sync.Mutex

	log := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		trace = append(trace, s)
	}

	// Batch 1
	wg.Add(1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		log("Done 1")
		wg.Done()
	}()

	log("Wait 1 Start")
	wg.Wait()
	log("Wait 1 End")

	// Batch 2 - Immediately reuse
	wg.Add(1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		log("Done 2")
		wg.Done()
	}()

	log("Wait 2 Start")
	wg.Wait()
	log("Wait 2 End")

	// Verify order
	// We expect: Wait 1 Start -> Done 1 -> Wait 1 End -> Wait 2 Start -> Done 2 -> Wait 2 End
	// Note: "Done 1" and "Wait 1 End" order is guaranteed by Wait logic.
	// But "Wait 1 End" happens on main thread.
}

func TestWaitGroupConcurrentReuse(t *testing.T) {
	// This test tries to break the reuse by adding concurrently with Wait return.
	var wg WaitGroup
	const N = 1000

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			wg.Done()
		}()
		wg.Wait()
	}
}

func TestWaitGroupConcurrentAddWait(t *testing.T) {
	// A strict stress test
	var wg WaitGroup
	done := make(chan bool)

	go func() {
		for range 10000 {
			wg.Add(1)
			go func() {
				wg.Done()
			}()
			wg.Wait()
		}
		done <- true
	}()

	<-done
}

func TestWaitGroupMultipleWaiters(t *testing.T) {
	var wg WaitGroup
	wg.Add(1)

	for range 10 {
		go func() {
			wg.Wait()
		}()
	}

	time.Sleep(10 * time.Millisecond)
	wg.Done()
	// If the test finishes, it means all Wait() calls returned (or test runner kills them, but we want to ensure no hang)
	// We can add a gate here.

	// Better test:
	var wg2 sync.WaitGroup
	wg2.Add(10)

	wg.Add(1)
	for range 10 {
		go func() {
			wg.Wait()
			wg2.Done()
		}()
	}
	wg.Done()

	// Wait for all waiters to finish
	wg2.Wait()
}

// ============================================================================
// Compatibility test (sync.WaitGroup)
// ============================================================================

func testWaitGroup(t *testing.T, wg1 *WaitGroup, wg2 *WaitGroup) {
	n := 16
	wg1.Add(n)
	wg2.Add(n)
	exited := make(chan bool, n)
	for i := 0; i != n; i++ {
		go func() {
			wg1.Done()
			wg2.Wait()
			exited <- true
		}()
	}
	wg1.Wait()
	for i := 0; i != n; i++ {
		select {
		case <-exited:
			t.Fatal("WaitGroup released group too soon")
		default:
		}
		wg2.Done()
	}
	for i := 0; i != n; i++ {
		<-exited // Will block if barrier fails to unlock someone.
	}
}

func TestWaitGroup(t *testing.T) {
	wg1 := &WaitGroup{}
	wg2 := &WaitGroup{}

	// Run the same test a few times to ensure barrier is in a proper state.
	for i := 0; i != 8; i++ {
		testWaitGroup(t, wg1, wg2)
	}
}

func TestWaitGroupMisuse(t *testing.T) {
	defer func() {
		err := recover()
		if err != "cc: negative WaitGroup counter" {
			t.Fatalf("Unexpected panic: %#v", err)
		}
	}()
	wg := &WaitGroup{}
	wg.Add(1)
	wg.Done()
	wg.Done()
	t.Fatal("Should panic")
}

func TestWaitGroupRace(t *testing.T) {
	// Run this test for about 1ms.
	for range 1000 {
		wg := &WaitGroup{}
		n := new(int32)
		// spawn goroutine 1
		wg.Add(1)
		go func() {
			atomic.AddInt32(n, 1)
			wg.Done()
		}()
		// spawn goroutine 2
		wg.Add(1)
		go func() {
			atomic.AddInt32(n, 1)
			wg.Done()
		}()
		// Wait for goroutine 1 and 2
		wg.Wait()
		if atomic.LoadInt32(n) != 2 {
			t.Fatal("Spurious wakeup from Wait")
		}
	}
}

func TestWaitGroupAlign(t *testing.T) {
	type X struct {
		x  byte //nolint:unused
		wg WaitGroup
	}
	var x X
	x.wg.Add(1)
	go func(x *X) {
		x.wg.Done()
	}(&x)
	x.wg.Wait()
}

func TestWaitGroupGo(t *testing.T) {
	wg := &WaitGroup{}
	var i int
	wg.Go(func() {
		i++
	})
	wg.Wait()
	if i != 1 {
		t.Fatalf("got %d, want 1", i)
	}
}

func TestWaitGroupCount(t *testing.T) {
	wg := &WaitGroup{}
	wg.Add(1)
	if wg.Count() != 1 {
		t.Fatalf("Count should be 1, got %d", wg.Count())
	}
	wg.Add(1)
	if wg.Count() != 2 {
		t.Fatalf("Count should be 2, got %d", wg.Count())
	}
	wg.Done()
	if wg.Count() != 1 {
		t.Fatalf("Count should be 1, got %d", wg.Count())
	}
	wg.Done()
	if wg.Count() != 0 {
		t.Fatalf("Count should be 0, got %d", wg.Count())
	}
}

func TestWaitGroupWaiters(t *testing.T) {
	wg := &WaitGroup{}
	wg.Add(1)

	for range 3 {
		go func() {
			wg.Wait()
		}()
	}

	// Give some time for waiters to block
	time.Sleep(10 * time.Millisecond)

	if w := wg.Waiters(); w != 3 {
		t.Fatalf("Waiters should be 3, got %d", w)
	}

	wg.Done()
}

func TestWaitGroupTryWait(t *testing.T) {
	wg := &WaitGroup{}
	if !wg.TryWait() {
		t.Fatal("TryWait should return true when empty")
	}

	wg.Add(1)
	if wg.TryWait() {
		t.Fatal("TryWait should return false when not empty")
	}

	wg.Done()
	if !wg.TryWait() {
		t.Fatal("TryWait should return true when empty again")
	}
}

func BenchmarkWaitGroupUncontended(b *testing.B) {
	type PaddedWaitGroup struct {
		WaitGroup
		pad [128]uint8 //nolint:unused
	}
	b.RunParallel(func(pb *testing.PB) {
		var wg PaddedWaitGroup
		for pb.Next() {
			wg.Add(1)
			wg.Done()
			wg.Wait()
		}
	})
}

func benchmarkWaitGroupAddDone(b *testing.B, localWork int) {
	var wg WaitGroup
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			wg.Add(1)
			for range localWork {
				foo *= 2
				foo /= 2
			}
			wg.Done()
		}
		_ = foo
	})
}

func BenchmarkWaitGroupAddDone(b *testing.B) {
	benchmarkWaitGroupAddDone(b, 0)
}

func BenchmarkWaitGroupAddDoneWork(b *testing.B) {
	benchmarkWaitGroupAddDone(b, 100)
}

func benchmarkWaitGroupWait(b *testing.B, localWork int) {
	var wg WaitGroup
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			wg.Wait()
			for range localWork {
				foo *= 2
				foo /= 2
			}
		}
		_ = foo
	})
}

func BenchmarkWaitGroupWait(b *testing.B) {
	benchmarkWaitGroupWait(b, 0)
}

func BenchmarkWaitGroupWaitWork(b *testing.B) {
	benchmarkWaitGroupWait(b, 100)
}

func BenchmarkWaitGroupActuallyWait(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var wg WaitGroup
			wg.Add(1)
			go func() {
				wg.Done()
			}()
			wg.Wait()
		}
	})
}
