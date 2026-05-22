package cc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

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
			s := *shards.slice.At(uintptr(pid))
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
			s := *shards.slice.At(uintptr(pid))
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
			s := *shards.slice.At(uintptr(pid))
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
			s := *shards.slice.At(uintptr(pid))
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
	var currentLen uintptr
	if current != nil {
		currentLen = uintptr(current.len)
	}
	if uintptr(needed) <= currentLen {
		p.mu.Unlock()
		return // Already grown by someone else
	}

	// The Go scheduler guarantees that pid < GOMAXPROCS in steady state.
	// Similar to sync.Pool, we size the array exactly to GOMAXPROCS to be "just right",
	// avoiding wasteful doubling while keeping the number of grows minimal.
	newSize := max(uintptr(needed), uintptr(runtime.GOMAXPROCS(0)))

	newShards := makeUnsafeSlice[*pLocalSlot[T]](newSize)
	if current != nil {
		for i := range currentLen {
			*newShards.At(i) = *current.slice.At(i)
		}
	}

	// Allocate new slots in a contiguous block for better locality
	addedCount := newSize - currentLen
	newBacking := makeUnsafeSlice[pLocalSlot[T]](addedCount)

	for i := range addedCount {
		if p.provider != nil {
			newBacking.At(i).val = p.provider()
		}
		// Calculate the target index in the new shards slice
		idx := currentLen + i
		// Store the pointer to the slot in the backing array
		*newShards.At(idx) = newBacking.At(i)
	}

	p.shards.Store(&pLocalShards[T]{
		slice: newShards,
		len:   int(newSize),
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
	if shards == nil {
		return
	}
	for i := range uintptr(shards.len) {
		s := *shards.slice.At(i)
		fn(&s.val)
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
			s := *shards.slice.At(uintptr(pid))
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
	if shards == nil {
		return 0
	}
	var sum uintptr
	for i := range uintptr(shards.len) {
		s := *shards.slice.At(i)
		sum += s.val.Load()
	}
	return sum
}

// Reset atomically reads the current value and resets all shards to zero.
// This is useful for periodic metric collection cycles.
func (p *PLocalCounter) Reset() uintptr {
	shards := p.shards.Load()
	if shards == nil {
		return 0
	}
	var sum uintptr
	for i := range uintptr(shards.len) {
		s := *shards.slice.At(i)
		sum += s.val.Swap(0)
	}
	return sum
}

// =============================================================================
// PLocalCounterN
// =============================================================================

// PLocalCounterNLen is the number of uintptr counters packed into each
// PLocalCounterN P-local cache-line slot.
const PLocalCounterNLen = int(opt.CacheLineSize_ / unsafe.Sizeof(atomic.Uintptr{}))

// PLocalCounterN is a P-local group of uintptr counters.
//
// Each P owns one cache-line-aligned slot containing PLocalCounterNLen counters.
// This is useful when a hot path needs several related counters but should still
// pay for only one cache line per P.
//
// The zero value of PLocalCounterN is ready to use.
type PLocalCounterN struct {
	shards atomic.Pointer[pLocalCounterNShards]
	mu     sync.Mutex // protects grow
}

type pLocalCounterNShards struct {
	slice    unsafeSlice[*pLocalCounterNSlot]
	len      int
	backings [][]byte
}

type pLocalCounterNSlot struct {
	counters [PLocalCounterNLen]atomic.Uintptr
}

func (p *pLocalCounterNSlot) slot(i int) *atomic.Uintptr {
	return (*atomic.Uintptr)(unsafe.Add(unsafe.Pointer(&p.counters), uintptr(i)*unsafe.Sizeof(atomic.Uintptr{})))
}

// NewPLocalCounterN creates a new PLocalCounterN instance.
//
// The zero value of PLocalCounterN is also usable.
func NewPLocalCounterN() *PLocalCounterN {
	p := &PLocalCounterN{}
	p.grow(runtime.GOMAXPROCS(0))
	return p
}

// Add adds delta to counter i in the current P-local cache-line slot.
func (p *PLocalCounterN) Add(i int, delta uintptr) {
	shards := p.shards.Load()
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(uintptr(pid))
			s.slot(i).Add(delta)
			runtime_procUnpin()
			return
		}
		runtime_procUnpin()
	}
	p.slowGet().slot(i).Add(delta)
}

func (p *PLocalCounterN) Add2(i int, deltaI uintptr, j int, deltaJ uintptr) {
	shards := p.shards.Load()
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(uintptr(pid))
			s.slot(i).Add(deltaI)
			s.slot(j).Add(deltaJ)
			runtime_procUnpin()
			return
		}
		runtime_procUnpin()
	}
	s := p.slowGet()
	s.slot(i).Add(deltaI)
	s.slot(j).Add(deltaJ)
}

// Get returns counter i from the current P-local cache-line slot.
func (p *PLocalCounterN) Get(i int) uintptr {
	shards := p.shards.Load()
	// Fast path: if shards exist
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(uintptr(pid))
			runtime_procUnpin()
			return s.slot(i).Load()
		}
		runtime_procUnpin()
	}

	// Slow path: grow
	return p.slowGet().slot(i).Load()
}

func (p *PLocalCounterN) slowGet() *pLocalCounterNSlot {
	for {
		pid := runtime_procPin()
		shards := p.shards.Load()
		if shards == nil {
			runtime_procUnpin()
			p.grow(pid + 1)
			continue
		}
		if pid < shards.len {
			s := *shards.slice.At(uintptr(pid))
			runtime_procUnpin()
			return s
		}
		runtime_procUnpin()
		p.grow(pid + 1)
	}
}

func (p *PLocalCounterN) grow(needed int) {
	p.mu.Lock()

	current := p.shards.Load()
	var currentLen uintptr
	if current != nil {
		currentLen = uintptr(current.len)
	}
	if uintptr(needed) <= currentLen {
		p.mu.Unlock()
		return
	}

	newSize := max(uintptr(needed), uintptr(runtime.GOMAXPROCS(0)))
	newShards := makeUnsafeSlice[*pLocalCounterNSlot](newSize)
	var backings [][]byte
	if current != nil {
		for i := range currentLen {
			*newShards.At(i) = *current.slice.At(i)
		}
		backings = make([][]byte, len(current.backings), len(current.backings)+1)
		copy(backings, current.backings)
	}

	addedCount := newSize - currentLen
	newBacking := make([]byte, addedCount*opt.CacheLineSize_+opt.CacheLineSize_-1)
	basePtr := unsafe.Pointer(unsafe.SliceData(newBacking))
	base := uintptr(basePtr)
	aligned := (base + opt.CacheLineSize_ - 1) &^ (opt.CacheLineSize_ - 1)
	offset := aligned - base
	for i := range addedCount {
		idx := currentLen + i
		*newShards.At(idx) = (*pLocalCounterNSlot)(unsafe.Add(basePtr, offset+i*opt.CacheLineSize_))
	}
	backings = append(backings, newBacking)

	p.shards.Store(&pLocalCounterNShards{
		slice:    newShards,
		len:      int(newSize),
		backings: backings,
	})
	p.mu.Unlock()
}

// Value returns the aggregated value of counter i across all P-local slots.
//
// Note: The result is an approximation if concurrent Adds are happening.
func (p *PLocalCounterN) Value(i int) uintptr {
	shards := p.shards.Load()
	if shards == nil {
		return 0
	}
	var sum uintptr
	for j := range uintptr(shards.len) {
		s := *shards.slice.At(j)
		sum += s.slot(i).Load()
	}
	return sum
}

// Reset atomically reads counter i and resets it to zero in all P-local slots.
func (p *PLocalCounterN) Reset(i int) uintptr {
	shards := p.shards.Load()
	if shards == nil {
		return 0
	}
	var sum uintptr
	for j := range uintptr(shards.len) {
		s := *shards.slice.At(j)
		sum += s.slot(i).Swap(0)
	}
	return sum
}

// Clear discards all P-local counter slots.
// Subsequent accesses lazily allocate fresh zeroed slots.
func (p *PLocalCounterN) Clear() {
	p.mu.Lock()
	p.shards.Store(nil)
	p.mu.Unlock()
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
			s := *shards.slice.At(uintptr(pid))
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
	if shards == nil {
		return 0
	}
	var sum uint64
	for i := range uintptr(shards.len) {
		s := *shards.slice.At(i)
		sum += s.val.Load()
	}
	return sum
}

// Reset atomically reads the current value and resets all shards to zero.
// This is useful for periodic metric collection cycles.
func (p *PLocalCounter64) Reset() uint64 {
	shards := p.shards.Load()
	if shards == nil {
		return 0
	}
	var sum uint64
	for i := range uintptr(shards.len) {
		s := *shards.slice.At(i)
		sum += s.val.Swap(0)
	}
	return sum
}

// =============================================================================
// PLocalCounter64N
// =============================================================================

// PLocalCounter64NLen is the number of uint64 counters packed into each
// PLocalCounter64N P-local cache-line slot.
const PLocalCounter64NLen = int(opt.CacheLineSize_ / unsafe.Sizeof(atomic.Uint64{}))

// PLocalCounter64N is a P-local group of uint64 counters.
//
// Each P owns one cache-line-aligned slot containing PLocalCounter64NLen counters.
// This is useful when a hot path needs several related uint64 counters but should
// still pay for only one cache line per P.
//
// The zero value of PLocalCounter64N is ready to use.
type PLocalCounter64N struct {
	shards atomic.Pointer[pLocalCounter64NShards]
	mu     sync.Mutex // protects grow
}

type pLocalCounter64NShards struct {
	slice    unsafeSlice[*pLocalCounter64NSlot]
	len      int
	backings [][]byte
}

type pLocalCounter64NSlot struct {
	counters [PLocalCounter64NLen]atomic.Uint64
}

func (p *pLocalCounter64NSlot) slot(i int) *atomic.Uint64 {
	return (*atomic.Uint64)(unsafe.Add(unsafe.Pointer(&p.counters), uintptr(i)*unsafe.Sizeof(atomic.Uint64{})))
}

// NewPLocalCounter64N creates a new PLocalCounter64N instance.
//
// The zero value of PLocalCounter64N is also usable.
func NewPLocalCounter64N() *PLocalCounter64N {
	p := &PLocalCounter64N{}
	p.grow(runtime.GOMAXPROCS(0))
	return p
}

// Add adds delta to counter i in the current P-local cache-line slot.
func (p *PLocalCounter64N) Add(i int, delta uint64) {
	shards := p.shards.Load()
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(uintptr(pid))
			s.slot(i).Add(delta)
			runtime_procUnpin()
			return
		}
		runtime_procUnpin()
	}
	p.slowGet().slot(i).Add(delta)
}

func (p *PLocalCounter64N) Add2(i int, deltaI uint64, j int, deltaJ uint64) {
	shards := p.shards.Load()
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(uintptr(pid))
			s.slot(i).Add(deltaI)
			s.slot(j).Add(deltaJ)
			runtime_procUnpin()
			return
		}
		runtime_procUnpin()
	}
	s := p.slowGet()
	s.slot(i).Add(deltaI)
	s.slot(j).Add(deltaJ)
}

// Get returns counter i from the current P-local cache-line slot.
func (p *PLocalCounter64N) Get(i int) uint64 {
	shards := p.shards.Load()
	// Fast path: if shards exist
	if shards != nil {
		pid := runtime_procPin()
		if pid < shards.len {
			s := *shards.slice.At(uintptr(pid))
			runtime_procUnpin()
			return s.slot(i).Load()
		}
		runtime_procUnpin()
	}

	// Slow path: grow
	return p.slowGet().slot(i).Load()
}

func (p *PLocalCounter64N) slowGet() *pLocalCounter64NSlot {
	for {
		pid := runtime_procPin()
		shards := p.shards.Load()
		if shards == nil {
			runtime_procUnpin()
			p.grow(pid + 1)
			continue
		}
		if pid < shards.len {
			s := *shards.slice.At(uintptr(pid))
			runtime_procUnpin()
			return s
		}
		runtime_procUnpin()
		p.grow(pid + 1)
	}
}

func (p *PLocalCounter64N) grow(needed int) {
	p.mu.Lock()

	current := p.shards.Load()
	var currentLen uintptr
	if current != nil {
		currentLen = uintptr(current.len)
	}
	if uintptr(needed) <= currentLen {
		p.mu.Unlock()
		return
	}

	newSize := max(uintptr(needed), uintptr(runtime.GOMAXPROCS(0)))
	newShards := makeUnsafeSlice[*pLocalCounter64NSlot](newSize)
	var backings [][]byte
	if current != nil {
		for i := range currentLen {
			*newShards.At(i) = *current.slice.At(i)
		}
		backings = make([][]byte, len(current.backings), len(current.backings)+1)
		copy(backings, current.backings)
	}

	addedCount := newSize - currentLen
	newBacking := make([]byte, addedCount*opt.CacheLineSize_+opt.CacheLineSize_-1)
	basePtr := unsafe.Pointer(unsafe.SliceData(newBacking))
	base := uintptr(basePtr)
	aligned := (base + opt.CacheLineSize_ - 1) &^ (opt.CacheLineSize_ - 1)
	offset := aligned - base
	for i := range addedCount {
		idx := currentLen + i
		*newShards.At(idx) = (*pLocalCounter64NSlot)(unsafe.Add(basePtr, offset+i*opt.CacheLineSize_))
	}
	backings = append(backings, newBacking)

	p.shards.Store(&pLocalCounter64NShards{
		slice:    newShards,
		len:      int(newSize),
		backings: backings,
	})
	p.mu.Unlock()
}

// Value returns the aggregated value of counter i across all P-local slots.
//
// Note: The result is an approximation if concurrent Adds are happening.
func (p *PLocalCounter64N) Value(i int) uint64 {
	shards := p.shards.Load()
	if shards == nil {
		return 0
	}
	var sum uint64
	for j := range uintptr(shards.len) {
		s := *shards.slice.At(j)
		sum += s.slot(i).Load()
	}
	return sum
}

// Reset atomically reads counter i and resets it to zero in all P-local slots.
func (p *PLocalCounter64N) Reset(i int) uint64 {
	shards := p.shards.Load()
	if shards == nil {
		return 0
	}
	var sum uint64
	for j := range uintptr(shards.len) {
		s := *shards.slice.At(j)
		sum += s.slot(i).Swap(0)
	}
	return sum
}

// Clear discards all P-local counter slots.
// Subsequent accesses lazily allocate fresh zeroed slots.
func (p *PLocalCounter64N) Clear() {
	p.mu.Lock()
	p.shards.Store(nil)
	p.mu.Unlock()
}
