package benchmark

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/llxisdsh/cc"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sys/cpu"
)

// =============================================================================
// Benchmark
// =============================================================================
const (
	CacheLineSize_ = unsafe.Sizeof(cpu.CacheLinePad{})
	Padding_       = 1
)

type counterStripe struct {
	_ [(CacheLineSize_ - unsafe.Sizeof(struct {
		c uintptr
	}{})%CacheLineSize_) % CacheLineSize_ * Padding_]byte
	c uintptr // Counter value, accessed atomically
}

// unsafeSlice provides semi-ergonomic limited slice-like functionality
// without bounds checking for fixed sized slices.
type unsafeSlice[T any] struct {
	ptr unsafe.Pointer
}

// makeUnsafeSlice creates a new unsafeSlice with the specified length.
//
//go:nosplit
func makeUnsafeSlice[T any](len uintptr) unsafeSlice[T] {
	return unsafeSlice[T]{ptr: unsafe.Pointer(unsafe.SliceData(make([]T, len)))}
}

// toUnsafeSlice converts a pointer to a fixed-size slice/array into an unsafeSlice.
//
//go:nosplit
func toUnsafeSlice[T any](ptr *T) unsafeSlice[T] {
	return unsafeSlice[T]{ptr: unsafe.Pointer(ptr)}
}

// At returns a pointer to the i-th element of the slice without bounds checking.
//
//go:nosplit
func (s unsafeSlice[T]) At(i uintptr) *T {
	return (*T)(unsafe.Add(s.ptr, i*unsafe.Sizeof(*new(T))))
}

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

// 3. Sharded Counter (Simulating mapTable's strategy)
type PerfXsyncCounter struct {
	stripes *xsync.Counter
}

func NewPerfXsyncCounter() *PerfXsyncCounter {
	return &PerfXsyncCounter{
		stripes: xsync.NewCounter(),
	}
}

func (c *PerfXsyncCounter) Add(delta uintptr) {
	c.stripes.Add(int64(delta))
}

func (c *PerfXsyncCounter) Value() uintptr {
	return uintptr(c.stripes.Value())
}

// 4. PLocal Counter
type PerfPLocalCounter struct {
	p *cc.PLocal[uintptr]
}

func NewPerfPLocalCounter() *PerfPLocalCounter {
	return &PerfPLocalCounter{
		p: cc.NewPLocal(func() uintptr { return 0 }),
	}
}

func (c *PerfPLocalCounter) Add(delta uintptr) {
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

// 5. PLocalCounter Counter (Specialized)
type PerfPLocalUintptrCounter struct {
	p *cc.PLocalCounter
}

func NewPerfPLocalUintptrCounter() *PerfPLocalUintptrCounter {
	return &PerfPLocalUintptrCounter{
		p: cc.NewPLocalCounter(),
	}
}

func (c *PerfPLocalUintptrCounter) Add(delta uintptr) {
	c.p.Add(delta)
}

func (c *PerfPLocalUintptrCounter) Value() uintptr {
	return c.p.Value()
}

// --- Benchmarks ---

func BenchmarkMutexCounter_Add(b *testing.B) {
	c := &PerfMutexCounter{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkAtomicCounter_Add(b *testing.B) {
	c := &PerfAtomicCounter{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkShardedCounter_1xCPUs_Add(b *testing.B) {
	cpus := runtime.GOMAXPROCS(0)
	shards := nextPowOf2(cpus)
	c := NewPerfShardedCounter(uintptr(shards))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkShardedCounter_4xCPUs_Add(b *testing.B) {
	cpus := runtime.GOMAXPROCS(0)
	shards := nextPowOf2(cpus) * 4
	c := NewPerfShardedCounter(uintptr(shards))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkXsyncCounter_Add(b *testing.B) {
	c := NewPerfXsyncCounter()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkPLocal_Add(b *testing.B) {
	c := NewPerfPLocalCounter()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

func BenchmarkPLocalCounter_Add(b *testing.B) {
	c := NewPerfPLocalUintptrCounter()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}

//
// // Verify correctness
// func TestPerfCounters(t *testing.T) {
// 	const N = 1000
// 	var wg sync.WaitGroup
//
// 	c1 := &PerfMutexCounter{}
// 	c2 := &PerfAtomicCounter{}
// 	c3 := NewPerfShardedCounter(16)
// 	c4 := NewPerfXsyncCounter()
// 	c5 := NewPerfPLocalCounter()
// 	c6 := NewPerfPLocalUintptrCounter()
//
// 	runners := []struct {
// 		name string
// 		add  func(uintptr)
// 		val  func() uintptr
// 	}{
// 		{"Mutex", c1.Add, c1.Value},
// 		{"Atomic", c2.Add, c2.Value},
// 		{"Sharded", c3.Add, c3.Value},
// 		{"Xsync", c4.Add, c4.Value},
// 		{"PLocal", c5.Add, c5.Value},
// 		{"PLocalUintptr", c6.Add, c6.Value},
// 	}
//
// 	for _, r := range runners {
// 		t.Run(r.name, func(t *testing.T) {
// 			wg.Add(N)
// 			for range N {
// 				go func() {
// 					r.add(1)
// 					wg.Done()
// 				}()
// 			}
// 			wg.Wait()
// 			if got := r.val(); got != N {
// 				t.Errorf("Expected %d, got %d", N, got)
// 			}
// 		})
// 	}
// }

func BenchmarkXsyncCounter_Value(b *testing.B) {
	l := xsync.NewCounter()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = l.Value()
		}
	})
}

func BenchmarkPLocal_Value(b *testing.B) {
	l := cc.NewPLocal(func() uintptr { return 0 })
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var sum uintptr
			l.ForEach(func(v *uintptr) {
				sum += *v
			})
			_ = sum
		}
	})
}

func BenchmarkPLocalCounter_Value(b *testing.B) {
	l := cc.NewPLocalCounter()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = l.Value()
		}
	})
}

//
// func BenchmarkSyncPool_GetPut(b *testing.B) {
// 	p := sync.Pool{
// 		New: func() any { return 0 },
// 	}
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			v := p.Get()
// 			p.Put(v)
// 		}
// 	})
// }
