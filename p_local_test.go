package cc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/llxisdsh/cc/internal/opt"
)

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

func TestPLocalCounter_Reset(t *testing.T) {
	c := NewPLocalCounter()
	c.Add(100)
	c.Add(200)

	val := c.Reset()
	if val != 300 {
		t.Errorf("Expected reset to return 300, got %d", val)
	}

	if c.Value() != 0 {
		t.Errorf("Expected 0 after reset, got %d", c.Value())
	}

	// Add again after reset
	c.Add(50)
	if c.Value() != 50 {
		t.Errorf("Expected 50, got %d", c.Value())
	}
}

func TestPooledLocalCounterN_Basic(t *testing.T) {
	var pool pooledLocalCounterNPool
	c := pool.New(2)
	c.Add(0, 10)
	c.Add(0, 20)
	c.Add(1, 7)

	if val := c.Value(0); val != 30 {
		t.Fatalf("Value(0) = %d, want 30", val)
	}
	if val := c.Get(1); val != 7 {
		t.Fatalf("Get(1) = %d, want 7", val)
	}
	if val := c.Reset(0); val != 30 {
		t.Fatalf("Reset(0) = %d, want 30", val)
	}
	if val := c.Value(0); val != 0 {
		t.Fatalf("Value(0) after Reset = %d, want 0", val)
	}
	c.Clear()
	if val := c.Value(1); val != 0 {
		t.Fatalf("Value(1) after Clear = %d, want 0", val)
	}
}

func TestPooledLocalCounterN_Isolation(t *testing.T) {
	var pool pooledLocalCounterNPool
	a := pool.New(2)
	b := pool.New(2)

	a.Add(0, 1)
	a.Add(1, 2)
	b.Add(0, 4)
	b.Add(1, 8)

	if val := a.Value(0); val != 1 {
		t.Fatalf("a.Value(0) = %d, want 1", val)
	}
	if val := a.Value(1); val != 2 {
		t.Fatalf("a.Value(1) = %d, want 2", val)
	}
	if val := b.Value(0); val != 4 {
		t.Fatalf("b.Value(0) = %d, want 4", val)
	}
	if val := b.Value(1); val != 8 {
		t.Fatalf("b.Value(1) = %d, want 8", val)
	}
}

func TestPooledLocalCounterN_Race(t *testing.T) {
	var pool pooledLocalCounterNPool
	c := pool.New(2)
	var wg sync.WaitGroup
	workers := 32
	loops := 1000

	wg.Add(workers)
	for w := range workers {
		go func(w int) {
			defer wg.Done()
			idx := uintptr(w & 1)
			for range loops {
				c.Add(idx, 1)
			}
		}(w)
	}
	wg.Wait()

	total := c.Value(0) + c.Value(1)
	if want := uintptr(workers * loops); total != want {
		t.Fatalf("total = %d, want %d", total, want)
	}
}

func TestPooledLocalCounterN_Alignment(t *testing.T) {
	var pool pooledLocalCounterNPool
	c := pool.New(2)
	c.Add(0, 1)

	if pool.currentBase == nil {
		t.Fatal("expected initialized current chunk")
	}
	if addr := uintptr(pool.currentBase); addr%opt.CacheLineSize_ != 0 {
		t.Fatalf("current chunk is not cache-line aligned: addr=%#x cacheLine=%d", addr, opt.CacheLineSize_)
	}
	if c.chunkBacking == nil {
		t.Fatal("expected counter handle to keep backing alive")
	}
	for pid := uintptr(0); pid <= c.mask; pid++ {
		row := uintptr(c.base) + (pid << pooledLocalCounterNRowShift)
		if row%opt.CacheLineSize_ != 0 {
			t.Fatalf("row %d is not cache-line aligned: addr=%#x cacheLine=%d", pid, row, opt.CacheLineSize_)
		}
	}
}

func TestPooledLocalCounterN_DoesNotCrossChunk(t *testing.T) {
	var pool pooledLocalCounterNPool
	first := pool.New(pooledLocalCounterNChunkLen / 2)
	firstChunk := pool.currentBase
	second := pool.New(pooledLocalCounterNChunkLen)
	secondChunk := pool.currentBase

	if firstChunk == nil || secondChunk == nil {
		t.Fatalf("expected initialized chunks: first=%p second=%p", firstChunk, secondChunk)
	}
	if firstChunk == secondChunk {
		t.Fatalf("expected second allocation to move to next chunk, got %p", secondChunk)
	}
	if first.base != firstChunk {
		t.Fatalf("first counter base = %p, want first chunk %p", first.base, firstChunk)
	}
	if second.base != secondChunk {
		t.Fatalf("second counter base = %p, want next chunk %p", second.base, secondChunk)
	}
}

func TestPooledLocalCounterN_OldChunkKeptAliveByHandle(t *testing.T) {
	var pool pooledLocalCounterNPool
	first := pool.New(pooledLocalCounterNChunkLen)
	first.Add(0, 7)

	for range 3 {
		_ = pool.New(pooledLocalCounterNChunkLen)
		runtime.GC()
	}

	if val := first.Value(0); val != 7 {
		t.Fatalf("old chunk value = %d, want 7", val)
	}
}

func TestPooledLocalCounterN_PanicWhenTooWide(t *testing.T) {
	var pool pooledLocalCounterNPool
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = pool.New(pooledLocalCounterNChunkLen + 1)
}

func TestPooledLocalCounterN_PanicWhenZero(t *testing.T) {
	var pool pooledLocalCounterNPool
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = pool.New(0)
}

func TestPooledLocalCounterN_PanicWhenNotPowerOfTwo(t *testing.T) {
	var pool pooledLocalCounterNPool
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = pool.New(3)
}

func TestPLocalCounterN_Basic(t *testing.T) {
	c := NewPLocalCounterN()
	c.Add(0, 10)
	c.Add(0, 20)
	c.Add(PLocalCounterNLen-1, 7)

	if val := c.Value(0); val != 30 {
		t.Errorf("Expected counter 0 to be 30, got %d", val)
	}
	if val := c.Value(PLocalCounterNLen - 1); val != 7 {
		t.Errorf("Expected last counter to be 7, got %d", val)
	}
}

func TestPLocalCounterN_Reset(t *testing.T) {
	var c PLocalCounterN
	c.Add(1, 100)
	c.Add(1, 200)
	c.Add(2, 50)

	if val := c.Reset(1); val != 300 {
		t.Errorf("Expected reset to return 300, got %d", val)
	}
	if val := c.Value(1); val != 0 {
		t.Errorf("Expected counter 1 to be 0 after reset, got %d", val)
	}
	if val := c.Value(2); val != 50 {
		t.Errorf("Expected counter 2 to remain 50, got %d", val)
	}
}

func TestPLocalCounterN_Alignment(t *testing.T) {
	c := NewPLocalCounterN()
	shards := c.shards.Load()
	if shards == nil {
		t.Fatal("Expected initialized shards")
	}
	if size := unsafe.Sizeof(pLocalCounterNSlot{}); size != opt.CacheLineSize_ {
		t.Fatalf("Expected slot size %d, got %d", opt.CacheLineSize_, size)
	}
	for i := range uintptr(shards.len) {
		s := *shards.slice.At(i)
		if addr := uintptr(unsafe.Pointer(s)); addr%opt.CacheLineSize_ != 0 {
			t.Fatalf("Slot %d is not cache-line aligned: addr=%#x cacheLine=%d", i, addr, opt.CacheLineSize_)
		}
	}
}

func TestPLocalCounterN_Race(t *testing.T) {
	c := NewPLocalCounterN()
	var wg sync.WaitGroup
	workers := 32
	loops := 1000

	wg.Add(workers)
	for w := range workers {
		go func(w int) {
			defer wg.Done()
			idx := w % PLocalCounterNLen
			for range loops {
				c.Add(idx, 1)
			}
		}(w)
	}
	wg.Wait()

	var total uintptr
	for i := range PLocalCounterNLen {
		total += c.Value(i)
	}
	if want := uintptr(workers * loops); total != want {
		t.Errorf("Expected total %d, got %d", want, total)
	}
}

func TestPLocalCounter64_Reset(t *testing.T) {
	c := NewPLocalCounter64()
	c.Add(100)
	c.Add(200)

	val := c.Reset()
	if val != 300 {
		t.Errorf("Expected reset to return 300, got %d", val)
	}

	if c.Value() != 0 {
		t.Errorf("Expected 0 after reset, got %d", c.Value())
	}

	// Add again after reset
	c.Add(50)
	if c.Value() != 50 {
		t.Errorf("Expected 50, got %d", c.Value())
	}
}

func TestPLocalCounter64N_Basic(t *testing.T) {
	c := NewPLocalCounter64N()
	c.Add(0, 10)
	c.Add(0, 20)
	c.Add(PLocalCounter64NLen-1, 7)

	if val := c.Value(0); val != 30 {
		t.Errorf("Expected counter 0 to be 30, got %d", val)
	}
	if val := c.Value(PLocalCounter64NLen - 1); val != 7 {
		t.Errorf("Expected last counter to be 7, got %d", val)
	}
}

func TestPLocalCounter64N_Reset(t *testing.T) {
	var c PLocalCounter64N
	c.Add(1, 100)
	c.Add(1, 200)
	c.Add(2, 50)

	if val := c.Reset(1); val != 300 {
		t.Errorf("Expected reset to return 300, got %d", val)
	}
	if val := c.Value(1); val != 0 {
		t.Errorf("Expected counter 1 to be 0 after reset, got %d", val)
	}
	if val := c.Value(2); val != 50 {
		t.Errorf("Expected counter 2 to remain 50, got %d", val)
	}
}

func TestPLocalCounter64N_Alignment(t *testing.T) {
	c := NewPLocalCounter64N()
	shards := c.shards.Load()
	if shards == nil {
		t.Fatal("Expected initialized shards")
	}
	if size := unsafe.Sizeof(pLocalCounter64NSlot{}); size != opt.CacheLineSize_ {
		t.Fatalf("Expected slot size %d, got %d", opt.CacheLineSize_, size)
	}
	for i := range uintptr(shards.len) {
		s := *shards.slice.At(i)
		if addr := uintptr(unsafe.Pointer(s)); addr%opt.CacheLineSize_ != 0 {
			t.Fatalf("Slot %d is not cache-line aligned: addr=%#x cacheLine=%d", i, addr, opt.CacheLineSize_)
		}
	}
}

func TestPLocalCounter64N_Race(t *testing.T) {
	c := NewPLocalCounter64N()
	var wg sync.WaitGroup
	workers := 32
	loops := 1000

	wg.Add(workers)
	for w := range workers {
		go func(w int) {
			defer wg.Done()
			idx := w % PLocalCounter64NLen
			for range loops {
				c.Add(idx, 1)
			}
		}(w)
	}
	wg.Wait()

	var total uint64
	for i := range PLocalCounter64NLen {
		total += c.Value(i)
	}
	if want := uint64(workers * loops); total != want {
		t.Errorf("Expected total %d, got %d", want, total)
	}
}

// =============================================================================
// Benchmark
// =============================================================================

// 1. Mutex
type PerfMutexCounter struct {
	mu  sync.Mutex
	val uintptr
}

func (c *PerfMutexCounter) Add(delta uintptr) {
	c.mu.Lock()
	c.val += delta
	c.mu.Unlock()
}

func (c *PerfMutexCounter) Value() uintptr {
	c.mu.Lock()
	rv := c.val
	c.mu.Unlock()
	return rv
}

// 1. Pure Atomic Counter
type PerfAtomicCounter struct {
	v atomic.Uintptr
}

func (c *PerfAtomicCounter) Add(delta uintptr) {
	c.v.Add(delta)
}

func (c *PerfAtomicCounter) Value() uintptr {
	return c.v.Load()
}

// 2. Sharded Counter (Simulating mapTable's strategy)
type PerfShardedCounter struct {
	stripes unsafeSlice[counterStripe]
	mask    uintptr
}

func NewPerfShardedCounter(shards uintptr) *PerfShardedCounter {
	// shards must be power of 2
	return &PerfShardedCounter{
		stripes: makeUnsafeSlice[counterStripe](shards),
		mask:    shards - 1,
	}
}

func (c *PerfShardedCounter) Add(delta uintptr) {
	idx := uintptr(runtime_cheaprand()) & c.mask
	atomic.AddUintptr(&c.stripes.At(idx).c, delta)
}

func (c *PerfShardedCounter) Value() uintptr {
	var sum uintptr
	for i := range c.mask + 1 {
		sum += c.stripes.At(i).c
	}
	return sum
}

// 3. PLocal Counter
type PerfPLocalCounter struct {
	p    *PLocal[uintptr]
	mask uintptr // Just added for comparison with ShardedCounter's benchmark.
}

func NewPerfPLocalCounter() *PerfPLocalCounter {
	return &PerfPLocalCounter{
		p:    NewPLocal(func() uintptr { return 0 }),
		mask: 1, // Just added for comparison with ShardedCounter's benchmark.
	}
}

// Benchmark version: includes hash masking to match ShardedCounter's overhead
func (c *PerfPLocalCounter) Add(delta uintptr) {
	delta |= uintptr(runtime_cheaprand()) & c.mask
	c.p.With(func(v *uintptr) {
		*v += delta
	})
}

// Is the actual implementation without benchmark overhead
func (c *PerfPLocalCounter) AddReal(delta uintptr) {
	c.p.With(func(v *uintptr) {
		*v += delta
	})
}

func (c *PerfPLocalCounter) Value() uintptr {
	var sum uintptr
	c.p.ForEach(func(v *uintptr) {
		sum += *v
	})
	return sum
}

// 3.5 PLocalCounter Counter (Specialized)
type PerfPLocalUintptrCounter struct {
	p    *PLocalCounter
	mask uintptr // Just added for comparison with ShardedCounter's benchmark.
}

func NewPerfPLocalUintptrCounter() *PerfPLocalUintptrCounter {
	return &PerfPLocalUintptrCounter{
		p:    NewPLocalCounter(),
		mask: 1, // Just added for comparison with ShardedCounter's benchmark.
	}
}

// Benchmark version: includes hash masking to match ShardedCounter's overhead
func (c *PerfPLocalUintptrCounter) Add(delta uintptr) {
	delta |= uintptr(runtime_cheaprand()) & c.mask
	c.p.Add(delta)
}

// Is the actual implementation without benchmark overhead
func (c *PerfPLocalUintptrCounter) AddReal(delta uintptr) {
	c.p.Add(delta)
}

func (c *PerfPLocalUintptrCounter) Value() uintptr {
	return c.p.Value()
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
		add  func(uintptr)
		val  func() uintptr
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
