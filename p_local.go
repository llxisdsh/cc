package cc

import (
	"runtime"
	"sync"
	"sync/atomic"
	_ "unsafe"

	"github.com/llxisdsh/cc/internal/opt"
)

//go:linkname runtime_procPin runtime.procPin
//go:nosplit
func runtime_procPin() int

//go:linkname runtime_procUnpin runtime.procUnpin
//go:nosplit
func runtime_procUnpin()

// =============================================================================
// PLocal[T]
// =============================================================================

// PLocal implements a P-local (Processor-local) storage mechanism.
// It provides a mechanism to access P-specific data shards to minimize lock contention
// in high-concurrency scenarios.
// T is the type of the stored data.
// The zero value of PLocal is ready to use.
type PLocal[T any] struct {
	// shards is a pointer to a slice of pointers to slots.
	// We use pointers to slots so that resizing the slice (and copying pointers)
	// does not move the underlying slot values, avoiding data races with active users.
	shards   atomic.Pointer[pLocalShards[T]]
	provider func() T
	mu       sync.Mutex // protects grow
}

type pLocalShards[T any] struct {
	slice unsafeSlice[*pLocalSlot[T]]
	len   int
}

type pLocalSlot[T any] struct {
	opt.PLocalSlotLock
	val T
	// Padding to prevent false sharing.
	_ [opt.CacheLineSize_]byte
}

// NewPLocal creates a new PLocal instance with the given provider function.
// provider is called ONLY ONCE per P to create the initial value when a new slot
// is allocated (lazy initialization).
//
// DESIGN RATIONALE:
// We use `func() T` (factory) instead of `func(*T)` (initializer) for ergonomics.
// We do NOT call this function on every Get() for two reasons:
//  1. **Performance**: PLocal is designed for ~1ns latency. An extra function call,
//     even if inlined, adds overhead checking for nil or executing logic.
//  2. **Statefulness**: Many use cases (counters, RNGs, pre-allocated buffers)
//     rely on state persisting across calls on the same P. Forcing a reset
//     would break these patterns.
//
// If you need a "clean" value every time, you should reset it explicitly:
//
//	val := p.Get()
//	val.Reset() // User-defined reset logic, strictly explicit and inlinable.
func NewPLocal[T any](provider func() T) *PLocal[T] {
	p := &PLocal[T]{provider: provider}
	p.grow(runtime.GOMAXPROCS(0))
	return p
}

// Get returns the pointer to the P-local value.
//
// CAUTION: The returned pointer is strictly P-local. The caller MUST ensure
// that they do not use this pointer after yielding the processor (e.g.,
// blocking, syscall, channel send/recv, runtime.Gosched, etc.).
//
// Performance Note:
// This method returns the existing slot for the current P. It does NOT assert
// ownership or reset the value. This enables using PLocal for:
// - Accumulators (e.g., metrics counters)
// - Persistent buffers (reusing capacity)
// - Thread-local RNG states
//
// Usage pattern:
//
//	val := p.Get()
//	*val++ // Use immediately
//
// If you need to persist usage across yields, use With or copy the value.
// This method is designed for extreme low-latency scenarios where function
// call overhead of With is significant.
func (p *PLocal[T]) Get() *T {
	shards := p.shards.Load()
	// Fast path: if shards exist
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(pid)
			runtime_procUnpin()
			return &s.val
		}
		runtime_procUnpin()
	}

	// Slow path: grow
	return p.slowGet()
}

func (p *PLocal[T]) slowGet() *T {
	for {
		pid := runtime_procPin()
		shards := p.shards.Load()
		if shards == nil {
			runtime_procUnpin()
			p.grow(pid + 1)
			continue
		}
		if pid < shards.len {
			s := *shards.slice.At(pid)
			runtime_procUnpin()
			return &s.val
		}
		runtime_procUnpin()
		p.grow(pid + 1)
	}
}

// With executes fn with the P-local value for the current P.
// The goroutine is pinned to the P during the execution of fn to ensure the value
// remains local to that P.
//
// Notes: fn must not block or call functions that might yield the processor
// (e.g., I/O, channel operations, select, runtime.Gosched). Doing so while pinned
// can delay garbage collection and cause other system-wide pauses.
func (p *PLocal[T]) With(fn func(*T)) {
	shards := p.shards.Load()
	// Fast path: if shards exist
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(pid)
			if opt.Race_ {
				s.Lock()
			}
			fn(&s.val)
			if opt.Race_ {
				s.Unlock()
			}
			runtime_procUnpin()
			return
		}
		runtime_procUnpin()
	}

	// Slow path: retry loop
	p.slowWith(fn)
}

func (p *PLocal[T]) slowWith(fn func(*T)) {
	for {
		pid := runtime_procPin()
		shards := p.shards.Load()
		if shards == nil {
			runtime_procUnpin()
			p.grow(pid + 1)
			continue
		}
		if pid < shards.len {
			s := *shards.slice.At(pid)
			if opt.Race_ {
				s.Lock()
			}
			fn(&s.val)
			if opt.Race_ {
				s.Unlock()
			}
			runtime_procUnpin()
			return
		}
		runtime_procUnpin()
		p.grow(pid + 1)
	}
}

func (p *PLocal[T]) grow(needed int) {
	p.mu.Lock()

	current := p.shards.Load()
	currentLen := 0
	if current != nil {
		currentLen = current.len
	}
	if needed <= currentLen {
		p.mu.Unlock()
		return // Already grown by someone else
	}

	// Double the size or at least satisfy needed
	newSize := max(needed, currentLen*2)
	// If current is nil (first grow), ensure we start with at least GOMAXPROCS
	if current == nil {
		initN := runtime.GOMAXPROCS(0)
		if initN > newSize {
			newSize = initN
		}
	}

	newShards := make([]*pLocalSlot[T], newSize)
	if current != nil {
		for i := range currentLen {
			newShards[i] = *current.slice.At(i)
		}
	}

	// Allocate new slots in a contiguous block for better locality
	addedCount := newSize - currentLen
	newBacking := make([]pLocalSlot[T], addedCount)

	for i := range addedCount {
		if p.provider != nil {
			newBacking[i].val = p.provider()
		}
		// Calculate the target index in the new shards slice
		idx := currentLen + i
		// Store the pointer to the slot in the backing array
		newShards[idx] = &newBacking[i]
	}

	p.shards.Store(&pLocalShards[T]{
		slice: makeUnsafeSlice(newShards),
		len:   newSize,
	})
	p.mu.Unlock()
}

// ForEach iterates over all P-local slots and calls fn on each of them.
// The iteration is performed on the shards that existed when the call started.
// Note: If T is not thread-safe, accessing the value in fn while
// it is being modified in With (on another P) constitutes a data race.
// For counters, consider using atomic types (e.g. atomic.Int64) as T.
func (p *PLocal[T]) ForEach(fn func(*T)) {
	shards := p.shards.Load()
	if shards != nil {
		for i := range shards.len {
			s := *shards.slice.At(i)
			fn(&s.val)
		}
	}
}

// Clear discards all P-local data.
// Subsequent accesses via With will re-initialize the data using the init function.
// This is useful for resetting the state or when the data is no longer needed.
func (p *PLocal[T]) Clear() {
	p.mu.Lock()
	p.shards.Store(nil)
	p.mu.Unlock()
}

// =============================================================================
// PLocalCounter
// =============================================================================

// PLocalCounter is a specialized version of PLocal for uintptr counters.
// The zero value of PLocalCounter is ready to use.
type PLocalCounter struct {
	PLocal[atomic.Uintptr]
}

// NewPLocalCounter creates a new PLocalCounter instance.
// It pre-allocates shards to avoid allocation on the first access.
// The zero value of PLocalCounter is also usable.
func NewPLocalCounter() *PLocalCounter {
	p := &PLocalCounter{}
	p.grow(runtime.GOMAXPROCS(0))
	return p
}

// Add adds delta to the P-local counter.
// This is faster than With() because it avoids the callback overhead.
func (p *PLocalCounter) Add(delta uintptr) {
	shards := p.shards.Load()
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(pid)
			s.val.Add(delta)
			runtime_procUnpin()
			return
		}
		runtime_procUnpin()
	}
	p.slowGet().Add(delta)
}

// Value returns the aggregated value of the P-local counter across all shards.
// Note: The result is an approximation if concurrent Adds are happening.
func (p *PLocalCounter) Value() uintptr {
	shards := p.shards.Load()
	var sum uintptr
	if shards != nil {
		for i := range shards.len {
			s := *shards.slice.At(i)
			sum += s.val.Load()
		}
	}
	return sum
}

// Reset atomically reads the current value and resets all shards to zero.
// This is useful for periodic metric collection cycles.
func (p *PLocalCounter) Reset() uintptr {
	shards := p.shards.Load()
	var sum uintptr
	if shards != nil {
		for i := range shards.len {
			s := *shards.slice.At(i)
			sum += s.val.Swap(0)
		}
	}
	return sum
}

// =============================================================================
// PLocalCounter64
// =============================================================================

// PLocalCounter64 is a specialized version of PLocal for uint64 counters.
// The zero value of PLocalCounter64 is ready to use.
type PLocalCounter64 struct {
	PLocal[atomic.Uint64]
}

// NewPLocalCounter64 creates a new PLocalCounter64 instance.
// It pre-allocates shards to avoid allocation on the first access.
// The zero value of PLocalCounter64 is also usable.
func NewPLocalCounter64() *PLocalCounter64 {
	p := &PLocalCounter64{}
	p.grow(runtime.GOMAXPROCS(0))
	return p
}

// Add adds delta to the P-local counter.
// This is faster than With() because it avoids the callback overhead.
func (p *PLocalCounter64) Add(delta uint64) {
	shards := p.shards.Load()
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(pid)
			s.val.Add(delta)
			runtime_procUnpin()
			return
		}
		runtime_procUnpin()
	}
	p.slowGet().Add(delta)
}

// Value returns the aggregated value of the P-local counter across all shards.
// Note: The result is an approximation if concurrent Adds are happening.
func (p *PLocalCounter64) Value() uint64 {
	shards := p.shards.Load()
	var sum uint64
	if shards != nil {
		for i := range shards.len {
			s := *shards.slice.At(i)
			sum += s.val.Load()
		}
	}
	return sum
}

// Reset atomically reads the current value and resets all shards to zero.
// This is useful for periodic metric collection cycles.
func (p *PLocalCounter64) Reset() uint64 {
	shards := p.shards.Load()
	var sum uint64
	if shards != nil {
		for i := range shards.len {
			s := *shards.slice.At(i)
			sum += s.val.Swap(0)
		}
	}
	return sum
}
