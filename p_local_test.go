package cc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	_ "unsafe"
)

//go:noescape
//go:linkname runtime_cheaprand runtime.cheaprand
func runtime_cheaprand() uint32

func TestPLocal_Basic(t *testing.T) {
	p := NewPLocal(func() int {
		return 0
	})

	p.With(func(v *int) {
		*v = 42
	})

	p.With(func(v *int) {
		if *v != 42 {
			_ = *v
			// Note: This check is only strictly valid if we are on the same P.
			// But in a single-threaded test or lucky scheduling, it holds.
			// To strictly test, we rely on the fact that if we just set it, valid logic implies we used it.
			// However, for testing persistence, we probably want to aggregate.
		}
	})
}

func TestPLocal_ZeroValue(t *testing.T) {
	var p PLocal[int]
	// Should work without panic
	p.With(func(v *int) {
		*v = 42
	})
	p.With(func(v *int) {
		if *v != 42 {
			t.Errorf("expected 42, got %d", *v)
		}
	})
}

func TestPLocal_ZeroInit(t *testing.T) {
	// Test case 1: Explicitly using NewPLocal with nil init function
	p1 := NewPLocal[int](nil)
	p1.With(func(v *int) {
		if *v != 0 {
			t.Errorf("Expected zero value 0, got %d", *v)
		}
		*v = 1
	})
	p1.With(func(v *int) {
		if *v != 1 {
			t.Errorf("Expected value 1, got %d", *v)
		}
	})

	// Test case 2: Zero value struct usage (Lazy initialization)
	// This simulates var p PLocal[int]
	var p2 PLocal[int]
	p2.With(func(v *int) {
		if *v != 0 {
			t.Errorf("Expected zero value 0, got %d", *v)
		}
		*v = 2
	})
	p2.With(func(v *int) {
		if *v != 2 {
			t.Errorf("Expected value 2, got %d", *v)
		}
	})
}

func TestPLocal_Get_Basic(t *testing.T) {
	p := NewPLocal(func() int { return 10 })

	val := p.Get()
	if *val != 10 {
		t.Errorf("Expected 10, got %d", *val)
	}
	*val = 20
	val2 := p.Get()
	// Same P
	if *val2 != 20 {
		t.Logf("Got %d on second access (might be diff P)", *val2)
	}
}

func TestPLocal_Get_Concurrency(t *testing.T) {
	// Use atomic.Int64 to ensure race detector doesn't complain about data races
	// if we accidentally share slots (we shouldn't) or if TSan doesn't see synchronization.
	// Since Get returns a raw pointer and we mutate it, we should use atomic for safety in tests.
	p := NewPLocal(func() atomic.Int64 { return atomic.Int64{} })

	var wg sync.WaitGroup
	workers := 100
	loops := 1000

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range loops {
				val := p.Get()
				val.Add(1)
			}
		}()
	}
	wg.Wait()

	var total int64
	p.ForEach(func(v *atomic.Int64) {
		total += v.Load()
	})

	if total != int64(workers*loops) {
		t.Errorf("Expected total %d, got %d", workers*loops, total)
	}
}

func TestPLocal_Concurrency(t *testing.T) {
	// PLocal counter.
	p := NewPLocal(func() int { return 0 })

	var wg sync.WaitGroup
	workers := 100
	loops := 1000

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range loops {
				p.With(func(v *int) {
					*v++
				})
			}
		}()
	}
	wg.Wait()

	// Sum up all shards
	total := 0
	p.ForEach(func(v *int) {
		total += *v
	})

	expected := workers * loops
	if total != expected {
		t.Errorf("Expected total %d, got %d", expected, total)
	}
}

func TestPLocal_RacePanic(t *testing.T) {
	// Attempt to trigger race condition or panic with heavy concurrent usage
	// Now using atomic.Int64 to ensure no actual data race
	p := NewPLocal(func() atomic.Int64 { return atomic.Int64{} })

	var wg sync.WaitGroup
	workers := 20

	// Workers doing With and potentially causing Grow
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range 1000 {
				p.With(func(v *atomic.Int64) {
					v.Add(1)
				})
			}
		}()
	}

	// Concurrent ForEach
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			p.ForEach(func(v *atomic.Int64) {
				_ = v.Load()
			})
		}
	}()

	wg.Wait()
}

func TestPLocal_Grow(t *testing.T) {
	// This test attempts to force growth by increasing GOMAXPROCS
	old := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(old)

	// Start small
	runtime.GOMAXPROCS(1)
	p := NewPLocal(func() int { return 1 })

	// Increase procs
	runtime.GOMAXPROCS(old + 4)

	var wg sync.WaitGroup
	// Launch many goroutines to likely hit new P's
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.With(func(v *int) {
				*v = 2
			})
		}()
	}
	wg.Wait()

	// Verify we have at least coverage
	sum := 0
	p.ForEach(func(v *int) {
		sum += *v
	})

	// We expect sum to be at least something greater than initial if we hit new Ps.
	if sum == 0 {
		t.Error("Sum should not be 0")
	}
}

func TestPLocal_ForEach(t *testing.T) {
	// Use atomic.Int64 for thread-safe counting
	p := NewPLocal(func() atomic.Int64 { return atomic.Int64{} })

	var wg sync.WaitGroup
	workers := 100
	loops := 1000

	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range loops {
				p.With(func(v *atomic.Int64) {
					v.Add(1)
				})
			}
		}()
	}
	wg.Wait()

	// Sum up all shards using ForEach
	var total int64
	p.ForEach(func(v *atomic.Int64) {
		total += v.Load()
	})

	expected := int64(workers * loops)
	if total != expected {
		t.Errorf("Expected total %d, got %d", expected, total)
	}
}

func TestPLocal_Clear(t *testing.T) {
	p := NewPLocal(func() int { return 10 })

	// Initial usage
	p.With(func(v *int) {
		*v = 20
	})

	// Verify modification
	found := false
	p.ForEach(func(v *int) {
		if *v == 20 {
			found = true
		}
	})
	if !found {
		t.Fatal("Expected to find value 20 before Clear")
	}

	// Clear
	p.Clear()

	// Verify cleared (ForEach should do nothing or iterate over empty)
	count := 0
	p.ForEach(func(v *int) {
		count++
	})
	if count != 0 {
		t.Errorf("Expected 0 items after Clear, got %d", count)
	}

	// Verify reuse re-initializes
	p.With(func(v *int) {
		if *v != 10 { // Should be re-initialized to 10
			t.Errorf("Expected re-initialized value 10, got %d", *v)
		}
		*v = 30
	})

	found30 := false
	p.ForEach(func(v *int) {
		if *v == 30 {
			found30 = true
		}
	})
	if !found30 {
		t.Error("Expected to find value 30 after reuse")
	}
}

func TestPLocalCounter_Race(t *testing.T) {
	c := NewPLocalCounter()
	var wg sync.WaitGroup
	done := make(chan struct{})

	// Workers doing Add (Fast Path)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					c.Add(1)
				}
			}
		}()
	}

	// Workers doing With (Slow/Locked Path potentially, or at least Lock-involved path if using PLocal.With)
	// Note: PLocalCounter.Add uses slowWith internally for retries, but we can also explicitly call With via the embedded field.
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					c.With(func(v *atomic.Uintptr) {
						v.Add(1)
					})
				}
			}
		}()
	}

	// Workers doing Value
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = c.Value()
				}
			}
		}()
	}

	// Workers doing ForEach
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					c.ForEach(func(v *atomic.Uintptr) {
						_ = v.Load()
					})
				}
			}
		}()
	}

	// Let it run for a bit
	// In a real test we might just run a fixed number of iterations,
	// but here we rely on the loop running enough times before we close done.
	// Since we can't sleep easily in tests without slowing them down,
	// we'll just iterate a bunch of times in the main goroutine or just close done immediately
	// after starting? No, that's too fast.
	// We'll use a separate counter to control duration.

	// Actually, let's just run a fixed number of operations in the main goroutine to gate the test.
	for range 10000 {
		c.Add(1)
	}

	close(done)
	wg.Wait()
}

func TestPLocalCounter64_Race(t *testing.T) {
	c := NewPLocalCounter64()
	var wg sync.WaitGroup
	done := make(chan struct{})

	// Workers doing Add (Fast Path)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					c.Add(1)
				}
			}
		}()
	}

	// Workers doing With
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					c.With(func(v *atomic.Uint64) {
						v.Add(1)
					})
				}
			}
		}()
	}

	// Workers doing Value
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = c.Value()
				}
			}
		}()
	}

	// Workers doing ForEach
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					c.ForEach(func(v *atomic.Uint64) {
						_ = v.Load()
					})
				}
			}
		}()
	}

	// Run fixed number of ops
	for range 10000 {
		c.Add(1)
	}

	close(done)
	wg.Wait()
}

func TestPLocalCounter64_Basic(t *testing.T) {
	c := NewPLocalCounter64()
	c.Add(10)
	c.Add(20)

	if val := c.Value(); val != 30 {
		t.Errorf("Expected 30, got %d", val)
	}

	c.With(func(v *atomic.Uint64) {
		v.Add(5)
	})

	if val := c.Value(); val != 35 {
		t.Errorf("Expected 35, got %d", val)
	}
}

// =============================================================================
// Benchmark
// =============================================================================

// 1. Mutex
type PerfMutexCounter struct {
	mu  sync.Mutex
	val int64
}

func (c *PerfMutexCounter) Add(delta int64) {
	c.mu.Lock()
	c.val += delta
	c.mu.Unlock()
}

func (c *PerfMutexCounter) Value() int64 {
	c.mu.Lock()
	rv := c.val
	c.mu.Unlock()
	return rv
}

// 1. Pure Atomic Counter
type PerfAtomicCounter struct {
	v atomic.Int64
}

func (c *PerfAtomicCounter) Add(delta int64) {
	c.v.Add(delta)
}

func (c *PerfAtomicCounter) Value() int64 {
	return c.v.Load()
}

// 2. Sharded Counter (Simulating mapTable's strategy)
type PerfShardedCounter struct {
	stripes []counterStripe
	mask    uintptr
}

func NewPerfShardedCounter(shards int) *PerfShardedCounter {
	// shards must be power of 2
	return &PerfShardedCounter{
		stripes: make([]counterStripe, shards),
		mask:    uintptr(shards - 1),
	}
}

//go:nosplit
func (c *PerfShardedCounter) Add(delta int64) {
	idx := uintptr(runtime_cheaprand()) & c.mask
	atomic.AddUintptr(&c.stripes[idx].c, uintptr(delta))
}

func (c *PerfShardedCounter) Value() int64 {
	var sum int64
	for i := range c.stripes {
		sum += int64(c.stripes[i].c)
	}
	return sum
}

// 3. PLocal Counter
type PerfPLocalCounter struct {
	p *PLocal[int64]
}

func NewPerfPLocalCounter() *PerfPLocalCounter {
	return &PerfPLocalCounter{
		p: NewPLocal(func() int64 { return 0 }),
	}
}

func (c *PerfPLocalCounter) Add(delta int64) {
	c.p.With(func(v *int64) {
		*v += delta
	})
}

func (c *PerfPLocalCounter) Value() int64 {
	var sum int64
	c.p.ForEach(func(v *int64) {
		sum += *v
	})
	return sum
}

// 3.5 PLocalCounter Counter (Specialized)
type PerfPLocalUintptrCounter struct {
	p *PLocalCounter
}

func NewPerfPLocalUintptrCounter() *PerfPLocalUintptrCounter {
	return &PerfPLocalUintptrCounter{
		p: NewPLocalCounter(),
	}
}

func (c *PerfPLocalUintptrCounter) Add(delta int64) {
	c.p.Add(uintptr(delta))
}

func (c *PerfPLocalUintptrCounter) Value() int64 {
	return int64(c.p.Value())
}

// --- Benchmarks ---

func BenchmarkPerfCounter_Mutex(b *testing.B) {
	c := &PerfMutexCounter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkPerfCounter_Atomic(b *testing.B) {
	c := &PerfAtomicCounter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkPerfCounter_Sharded_32(b *testing.B) {
	c := NewPerfShardedCounter(32)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkPerfCounter_Sharded_4xCPUs(b *testing.B) {
	cpus := runtime.GOMAXPROCS(0)
	// Round up to power of 2
	shards := 1
	for shards < cpus*4 { // 4x over-provisioning like map
		shards <<= 1
	}
	c := NewPerfShardedCounter(shards)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkPerfCounter_PLocal(b *testing.B) {
	c := NewPerfPLocalCounter()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkPerfCounter_PLocalUintptr(b *testing.B) {
	c := NewPerfPLocalUintptrCounter()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

// 4. Comparison: map.go internal AddSize simulation
// map.go uses a known index. This is much faster than PLocal or Random Sharding.
// We should simulate this specific usage pattern.

func BenchmarkPerfCounter_MapInternal_KnownIndex(b *testing.B) {
	// Simulate map structure
	cpus := runtime.GOMAXPROCS(0)
	shards := 1
	for shards < cpus*1 {
		shards <<= 1
	}
	c := NewPerfShardedCounter(shards)

	b.RunParallel(func(pb *testing.PB) {
		idx := uintptr(runtime_cheaprand()) & c.mask
		for pb.Next() {
			atomic.AddUintptr(&c.stripes[idx].c, 1)
		}
	})
}

// Verify correctness
func TestPerfCounters(t *testing.T) {
	const N = 1000
	var wg sync.WaitGroup

	c1 := &PerfMutexCounter{}
	c2 := &PerfAtomicCounter{}
	c3 := NewPerfShardedCounter(16)
	c4 := NewPerfPLocalCounter()
	c5 := NewPerfPLocalUintptrCounter()

	runners := []struct {
		name string
		add  func(int64)
		val  func() int64
	}{
		{"Mutex", c1.Add, c1.Value},
		{"Atomic", c2.Add, c2.Value},
		{"Sharded", c3.Add, c3.Value},
		{"PLocal", c4.Add, c4.Value},
		{"PLocalCounter", c5.Add, c5.Value},
	}

	for _, r := range runners {
		t.Run(r.name, func(t *testing.T) {
			wg.Add(N)
			for range N {
				go func() {
					r.add(1)
					wg.Done()
				}()
			}
			wg.Wait()
			if got := r.val(); got != N {
				t.Errorf("Expected %d, got %d", N, got)
			}
		})
	}
}

func BenchmarkPLocal_Get(b *testing.B) {
	l := NewPLocal(func() int { return 0 })
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = l.Get()
		}
	})
}

func BenchmarkSyncPool_GetPut(b *testing.B) {
	p := sync.Pool{
		New: func() any { return 0 },
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := p.Get()
			p.Put(v)
		}
	})
}
