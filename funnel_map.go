package cc

import (
	"cmp"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/llxisdsh/cc/internal/opt"
)

type FunnelMap[K cmp.Ordered, V any] struct {
	_        noCopy
	table    unsafe.Pointer // *hMapTable
	rs       unsafe.Pointer // *hRebuildState
	seed     uintptr
	keyHash  HashFunc  // WithKeyHasher
	valEqual EqualFunc // WithValueEqual
	minLen   uintptr   // WithCapacity
	shrinkOn bool      // WithAutoShrink
	intKey   bool
}

// fRebuildState represents the current state of a resizing operation
type fRebuildState struct {
	hint      mapRebuildHint
	latch     Latch
	table     unsafe.Pointer // *hMapTable
	newTable  unsafe.Pointer // *hMapTable
	process   uint32
	completed uint32
}

// fMapTable represents the internal hash table structure.
type fMapTable[K cmp.Ordered, V any] struct {
	buckets unsafeSlice[fBucket]
	mask    uintptr
	size    PLocalCounter
	// number of chunks and chunks size for resizing
	chunks   uintptr
	overflow *SkipMap[K, V]
	// overflowWriters tracks in-flight lock-free overflow writers
	overflowWriters int32
}

// fBucket represents a hash table fBucket with cache-line alignment.
type fBucket struct {
	// meta: metadata for fast entry lookups, must be 64-bit aligned
	_       [0]atomic.Uint64
	meta    uint64
	entries [fEntriesPerBucket]unsafe.Pointer // *opt.Entry_
}

// NewFunnelMap creates a new HMap instance. Direct initialization is also
// supported.
//
// Parameters:
//   - options: configuration options (WithCapacity, WithKeyHasher, etc.)
func NewFunnelMap[K cmp.Ordered, V any](
	options ...func(*MapConfig),
) *FunnelMap[K, V] {
	m := &FunnelMap[K, V]{}
	m.withOptions(options...)
	return m
}

func (m *FunnelMap[K, V]) withOptions(
	options ...func(*MapConfig),
) {
	var cfg MapConfig

	// parse options
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m.init(noEscape(&cfg))
}

func (m *FunnelMap[K, V]) init(
	cfg *MapConfig,
) *fMapTable[K, V] {
	// parse interface
	if cfg.keyHash == nil {
		cfg.keyHash = parseKeyInterface[K]()
	}
	if cfg.valEqual == nil {
		cfg.valEqual = parseValueInterface[V]()
	}
	// perform initialization
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	if cfg.keyHash != nil {
		m.keyHash = cfg.keyHash
		m.intKey = false
	}
	if cfg.valEqual != nil {
		m.valEqual = cfg.valEqual
	}

	m.seed = uintptr(rand.Uint64())
	tableLen := calcTableLen(cfg.capacity)
	m.minLen = tableLen
	m.shrinkOn = cfg.autoShrink

	newTable := newFunnelMapTable[K, V](tableLen, maxProcs())
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))

	return newTable
}

func newFunnelMapTable[K cmp.Ordered, V any](tableLen, cpus uintptr) *fMapTable[K, V] {
	table := &fMapTable[K, V]{
		buckets:  makeUnsafeSlice[fBucket](tableLen),
		mask:     tableLen - 1,
		chunks:   calcParallelism(tableLen, minBucketsPerCPU, cpus*resizeOverPartition),
		overflow: NewSkipMap[K, V](),
	}
	return table
}

// Load retrieves a value for the given key.
//
//go:nosplit
func (m *FunnelMap[K, V]) Load(key K) (value V, ok bool) {
	table := (*fMapTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return *new(V), false
	}

	var hash uintptr
	var h1v uintptr

	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(&key)))
		h1v = hash / fEntriesPerBucket
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h1v = h1(hash)
	}

	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1v
	b := table.buckets.At(idx)
	meta := loadUint64(&b.meta)
	for marked := fMarkZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
		j := firstMarkedByteIndex(marked)
		if e := (*entry_[K, V])(loadPtr(b.At(j))); e != nil {
			if !opt.EmbeddedHash_ || e.GetHash() == hash {
				if e.key == key {
					return e.value, true
				}
			}
		}
	}
	if meta&opNextMask != 0 {
		return table.overflow.Load(key)
	}
	return *new(V), false
}

// Store inserts or updates a key-value pair.
func (m *FunnelMap[K, V]) Store(key K, value V) {
	table := (*fMapTable[K, V])(loadPtr(&m.table))
	if table == nil {
		table = m.slowInit()
	}

	var hash uintptr
	var h1v uintptr
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(&key)))
		h1v = hash / fEntriesPerBucket
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h1v = h1(hash)
	}
	h2v := h2(hash)
	h2w := broadcast(h2v)
	var fastOverflow bool

	// Fast path: lock-free read
	{
		idx := table.mask & h1v
		b := table.buckets.At(idx)

		meta := loadUint64(&b.meta)
		for marked := fMarkZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if e := (*entry_[K, V])(loadPtr(b.At(j))); e != nil {
				if !opt.EmbeddedHash_ || e.GetHash() == hash {
					if e.key == key {
						// valEqual: skip write if value unchanged
						if m.valEqual != nil {
							if m.valEqual(
								noescape(unsafe.Pointer(&e.value)),
								noescape(unsafe.Pointer(&value))) {
								return
							}
						}
						goto slowPath
					}
				}
			}
		}
		if meta&opNextMask != 0 {
			if v, ok := table.overflow.Load(key); ok {
				if m.valEqual != nil {
					if m.valEqual(
						noescape(unsafe.Pointer(&v)),
						noescape(unsafe.Pointer(&value))) {
						return
					}
				}
				fastOverflow = true
			}
		}
	}

slowPath:

	for {
		idx := table.mask & h1v
		b := table.buckets.At(idx)

		b.Lock()

		// This is the first check, checking if there is a rebuild operation in
		// progress before acquiring the Bucket lock
		if rs := (*fRebuildState)(loadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if loadPtr(&rs.newTable) != nil {
					b.Unlock()
					m.helpCopyAndWait(rs)
					table = (*fMapTable[K, V])(loadPtr(&m.table))
					continue
				}
			case mapRebuildBlockWritersHint:
				b.Unlock()
				rs.latch.Wait()
				table = (*fMapTable[K, V])(loadPtr(&m.table))
				continue
			default:
				// mapRebuildWithWritersHint: allow concurrent writers
			}
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := (*fMapTable[K, V])(loadPtr(&m.table)); table != newTable {
			b.Unlock()
			table = newTable
			continue
		}

		var (
			meta     uint64
			j        uintptr
			overflow bool
		)

		meta = loadUint64Fast(&b.meta)
		for marked := fMarkZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j = firstMarkedByteIndex(marked)
			e := (*entry_[K, V])(*b.At(j))
			if !opt.EmbeddedHash_ || e.GetHash() == hash {
				if e.key == key {
					goto found
				}
			}
		}

		if meta&opNextMask != 0 {
			if fastOverflow {
				overflow = true
			} else {
				_, overflow = table.overflow.Load(key)
			}
		}

		if !overflow {
			// Insert into empty slot
			if empty := (^meta) & fMetaMask; empty != 0 {
				// publish pointer first, then meta; readers check meta before
				// pointer so they won't observe a partially-initialized entry,
				// and this reduces the window where meta is visible but pointer is
				// still nil
				emptyIdx := firstMarkedByteIndex(empty)
				newEntry := &entry_[K, V]{key: key, value: value}
				if opt.EmbeddedHash_ {
					newEntry.SetHash(hash)
				}
				storePtr(b.At(emptyIdx), unsafe.Pointer(newEntry))
				newMeta := setByte(meta, h2v, emptyIdx)
				b.UnlockWithMeta(newMeta)
				table.AddSize(idx, 1)
				return
			}
		}

		// overflow
		atomic.AddInt32(&table.overflowWriters, 1)
		b.UnlockWithMeta(meta | opNextMask)

		table.overflow.Store(key, value)

		atomic.AddInt32(&table.overflowWriters, -1)

		// Check if the table needs to grow
		if loadPtr(&m.rs) == nil {
			tableLen := table.mask + 1
			size := table.SumSize() + uintptr(table.overflow.Size())
			const capFactor = float64(fEntriesPerBucket) * loadFactor
			if size >= uintptr(float64(tableLen)*capFactor) {
				m.tryResize(mapGrowHint, tableLen<<1)
			}
		}
		return

	found:
		// Update
		newEntry := &entry_[K, V]{key: key, value: value}
		if opt.EmbeddedHash_ {
			newEntry.SetHash(hash)
		}
		storePtr(b.At(j), unsafe.Pointer(newEntry))
		b.Unlock()
		return
	}
}

// Delete removes a key-value pair.
func (m *FunnelMap[K, V]) Delete(key K) {
	m.compute(&key, nil, computeSkipIfNotFound)
}

// LoadOrStore retrieves an existing value or stores a new one if the key
// doesn't exist.
func (m *FunnelMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.compute(&key, &value, computeInit|computeSkipIfFound)
}

// LoadAndUpdate retrieves the value associated with the given key and updates
// it if the key exists.
//
// Parameters:
//   - key: The key to look up in the map.
//   - value: The new value to set if the key exists.
//
// Returns:
//   - previous: The loaded value associated with the key (if it existed),
//     otherwise a zero-value of V.
//   - loaded: True if the key existed and the value was updated,
//     false otherwise.
func (m *FunnelMap[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	return m.compute(&key, &value, computeSkipIfNotFound)
}

// LoadAndDelete retrieves the value for a key and deletes it from the map.
func (m *FunnelMap[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.compute(&key, nil, computeSkipIfNotFound)
}

// Swap stores a key-value pair and returns the previous value if any.
func (m *FunnelMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	return m.compute(&key, &value, computeInit)
}

func (m *FunnelMap[K, V]) compute(
	key *K,
	val *V,
	flags uint8,
) (actual V, loaded bool) {
	table := (*fMapTable[K, V])(loadPtr(&m.table))
	if table == nil {
		if flags&computeInit == 0 {
			return *new(V), false
		}
		table = m.slowInit()
	}

	var hash uintptr
	var h1v uintptr
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(key)))
		h1v = hash / fEntriesPerBucket
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
		h1v = h1(hash)
	}
	h2v := h2(hash)
	h2w := broadcast(h2v)
	// Fast path: lock-free read
	if flags&(computeSkipIfFound|computeSkipIfNotFound) != 0 {
		idx := table.mask & h1v
		b := table.buckets.At(idx)
		meta := loadUint64(&b.meta)
		for marked := fMarkZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if e := (*entry_[K, V])(loadPtr(b.At(j))); e != nil {
				if !opt.EmbeddedHash_ || e.GetHash() == hash {
					if e.key == *key {
						if flags&computeSkipIfFound != 0 {
							return e.value, true
						}
						goto slowPath
					}
				}
			}
		}
		if meta&opNextMask != 0 {
			if v, ok := table.overflow.Load(*key); ok {
				if flags&computeSkipIfFound != 0 {
					return v, true
				}
				goto slowPath
			}
		}
		// Key not found in fast path
		if flags&computeSkipIfNotFound != 0 {
			return *new(V), false
		}
	}

slowPath:

	for {
		idx := table.mask & h1v
		b := table.buckets.At(idx)

		b.Lock()

		// This is the first check: verifies if a rebuild operation is in
		// progress AFTER acquiring the bucket lock. This strict ordering
		// ensures that copyBucket cannot silently migrate entries we are
		// about to mutate.
		if flags&computeIgnoreHint == 0 {
			if rs := (*fRebuildState)(loadPtr(&m.rs)); rs != nil {
				switch rs.hint {
				case mapGrowHint, mapShrinkHint:
					if loadPtr(&rs.newTable) != nil {
						b.Unlock()
						m.helpCopyAndWait(rs)
						table = (*fMapTable[K, V])(loadPtr(&m.table))
						continue
					}
				case mapRebuildBlockWritersHint:
					b.Unlock()
					rs.latch.Wait()
					table = (*fMapTable[K, V])(loadPtr(&m.table))
					continue
				default:
					// mapRebuildWithWritersHint: allow concurrent writers
				}
			}
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := (*fMapTable[K, V])(loadPtr(&m.table)); table != newTable {
			b.Unlock()
			table = newTable
			continue
		}

		var (
			meta     uint64
			j        uintptr
			overflow bool
		)
		it := MapEntry[K, V]{entry: entry_[K, V]{key: *key}}
		meta = loadUint64Fast(&b.meta)
		for marked := fMarkZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j = firstMarkedByteIndex(marked)
			e := (*entry_[K, V])(*b.At(j))
			if !opt.EmbeddedHash_ || e.GetHash() == hash {
				if e.key == *key {
					it.entry.value, it.loaded = e.value, true
					break
				}
			}
		}
		if !it.loaded {
			if meta&opNextMask != 0 {
				if v, ok := table.overflow.Load(*key); ok {
					it.entry.value, it.loaded = v, true
					overflow = true
				}
			}
		}

		// --- Compute Logic ---
		retV := it.entry.value
		if flags&(computeSkipIfFound|computeSkipIfNotFound) == 0 {
			// Store, Swap
			it.entry.value = *val
			it.op = updateOp
		} else if flags&(computeSkipIfFound) == 0 {
			// Delete, LoadAnd.., CompareAnd..
			if it.loaded {
				if val != nil {
					it.entry.value = *val
					it.op = updateOp
				} else {
					it.op = deleteOp
				}
			}
		} else /*if flags&computeUsesFunc == 0*/ {
			// LoadOr..
			if !it.loaded {
				it.entry.value = *val
				it.op = updateOp
				retV = it.entry.value
			}
		}
		// } else {
		// 	// Compute, LoadOrStoreFn
		// 	(*(*func(e *MapEntry[K, V]))(val))(noEscape(&it))
		// 	retV = it.entry.value
		// }

		switch it.op {
		case updateOp:
			if it.loaded {
				// valEqual: skip write if value unchanged
				if m.valEqual != nil {
					if m.valEqual(
						noescape(unsafe.Pointer(&((*entry_[K, V])(*b.At(j))).value)),
						noescape(unsafe.Pointer(&it.entry.value)),
					) {
						b.Unlock()
						return retV, it.loaded
					}
				}

				if overflow {
					atomic.AddInt32(&table.overflowWriters, 1)
					b.Unlock()
					table.overflow.Store(*key, it.entry.value)
					atomic.AddInt32(&table.overflowWriters, -1)
					return retV, it.loaded
				}

				// Update
				newEntry := &entry_[K, V]{key: *key, value: it.entry.value}
				if opt.EmbeddedHash_ {
					newEntry.SetHash(hash)
				}
				storePtr(b.At(j), unsafe.Pointer(newEntry))
				b.Unlock()
				return retV, it.loaded
			}

			// Insert into empty slot
			if empty := (^meta) & fMetaMask; empty != 0 {
				emptyIdx := firstMarkedByteIndex(empty)
				// publish pointer first, then meta; readers check meta before
				// pointer so they won't observe a partially-initialized entry,
				// and this reduces the window where meta is visible but pointer is
				// still nil
				newEntry := &entry_[K, V]{key: *key, value: it.entry.value}
				if opt.EmbeddedHash_ {
					newEntry.SetHash(hash)
				}
				storePtr(b.At(emptyIdx), unsafe.Pointer(newEntry))
				newMeta := setByte(meta, h2v, emptyIdx)
				b.UnlockWithMeta(newMeta)
				table.AddSize(idx, 1)
				return retV, it.loaded
			}
			atomic.AddInt32(&table.overflowWriters, 1)
			b.UnlockWithMeta(meta | opNextMask)
			table.overflow.Store(*key, it.entry.value)
			atomic.AddInt32(&table.overflowWriters, -1)

			// Check if the table needs to grow
			if loadPtr(&m.rs) == nil {
				tableLen := table.mask + 1
				size := table.SumSize() + uintptr(table.overflow.Size())
				const capFactor = float64(fEntriesPerBucket) * loadFactor
				if size >= uintptr(float64(tableLen)*capFactor) {
					m.tryResize(mapGrowHint, tableLen<<1)
				}
			}
			return retV, it.loaded
		case deleteOp:
			if !it.loaded {
				b.Unlock()
				return retV, it.loaded
			}
			if overflow {
				atomic.AddInt32(&table.overflowWriters, 1)
				b.Unlock()
				table.overflow.Delete(*key)
				atomic.AddInt32(&table.overflowWriters, -1)
				return retV, it.loaded
			}
			// Delete
			storePtr(b.At(j), nil)
			newMeta := setByte(meta, h2Empty, j)
			b.UnlockWithMeta(newMeta)
			table.AddSize(idx, ^uintptr(0))

			// Check if table shrinking is needed
			if m.shrinkOn {
				if newMeta&metaDataMask == metaEmpty {
					if loadPtr(&m.rs) == nil {
						tableLen := table.mask + 1
						if m.minLen < tableLen {
							size := table.SumSize() + uintptr(table.overflow.Size())
							if size < tableLen*fEntriesPerBucket/shrinkFraction {
								m.tryResize(mapShrinkHint, tableLen>>1)
							}
						}
					}
				}
			}
			return retV, it.loaded
		default:
			// cancelOp: no-op
			b.Unlock()
			return retV, it.loaded
		}
	}
}

// Range iterates all entries.
// Notes:
//   - The iteration directly traverses bucket data. The data is not guaranteed
//     to be real-time but provides eventual consistency.
//     In extreme cases, the same value may be traversed twice
//     (if it gets deleted and re-added later during iteration).
func (m *FunnelMap[K, V]) Range(yield func(key K, value V) bool) {
	table := (*fMapTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return
	}
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
		meta := loadUint64(&b.meta)
		for marked := meta & fMetaMask; marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if e := (*entry_[K, V])(loadPtr(b.At(j))); e != nil {
				if !yield(e.key, e.value) {
					return
				}
			}
		}
	}
	table.overflow.Range(yield)
}

// All returns an iterator function for use with range-over-func.
// It provides the same functionality as Range but in iterator form.
//
//go:nosplit
func (m *FunnelMap[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the number of key-value pairs in the map.
// This is an O(1) operation.
//
//go:nosplit
func (m *FunnelMap[K, V]) Size() int {
	table := (*fMapTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return 0
	}
	return int(table.SumSize()) + table.overflow.Size()
}

// Grow increases the map's capacity by sizeAdd entries to accommodate future
// growth. This pre-allocation avoids rehashing when adding new entries up to
// the new capacity.
//
// Parameters:
//   - sizeAdd specifies the number of additional entries the map should be able
//     to hold.
//
// Notes:
//   - If the current remaining capacity already exceeds sizeAdd, no growth will
//     be triggered.
func (m *FunnelMap[K, V]) Grow(sizeAdd int) {
	if sizeAdd <= 0 {
		return
	}
	if loadPtr(&m.table) == nil {
		m.slowInit()
	}
	m.doResize(mapGrowHint, uintptr(sizeAdd))
}

// Shrink reduces the capacity to fit the current size,
// always executes regardless of WithAutoShrink.
func (m *FunnelMap[K, V]) Shrink() {
	table := (*fMapTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return
	}
	m.doResize(mapShrinkHint, 0)
}

func (m *FunnelMap[K, V]) doResize(
	hint mapRebuildHint,
	sizeAdd uintptr,
) {
	for {
		// Resize check
		table := (*fMapTable[K, V])(loadPtr(&m.table))
		tableLen := table.mask + 1
		var newLen uintptr
		if hint == mapGrowHint {
			if sizeAdd <= 0 {
				return
			}
			newLen = calcTableLen(table.SumSize() + uintptr(table.overflow.Size()) + sizeAdd)
			if newLen <= tableLen {
				return
			}
		} else {
			// mapShrinkHint
			if tableLen <= m.minLen {
				return
			}
			newLen = calcTableLen(table.SumSize() + uintptr(table.overflow.Size()))
			if newLen >= tableLen {
				return
			}
		}
		// Help finishing rebuild if needed
		if rs := (*fRebuildState)(loadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if loadPtr(&rs.newTable) != nil {
					m.helpCopyAndWait(rs)
				} else {
					runtime.Gosched()
					continue
				}
			default:
				rs.latch.Wait()
			}
		}

		if m.tryResize(hint, newLen) {
			return
		}
	}
}

// slowInit may be called concurrently by multiple goroutines, so it requires
// synchronization with a "lock" mechanism.
//
//go:noinline
func (m *FunnelMap[K, V]) slowInit() *fMapTable[K, V] {
	rs := m.beginRebuild(mapRebuildBlockWritersHint)
	if rs == nil {
		// Another goroutine is initializing, wait for it to complete
		rs = (*fRebuildState)(loadPtr(&m.rs))
		if rs != nil {
			rs.latch.Wait()
		}
		// Now the table should be initialized
		return (*fMapTable[K, V])(loadPtr(&m.table))
	}

	// Although the table is always changed when rs is not nil,
	// it might have been changed before that.
	table := (*fMapTable[K, V])(loadPtr(&m.table))
	if table != nil {
		m.endRebuild(rs)
		return table
	}

	// Perform initialization
	var cfg MapConfig
	table = m.init(&cfg)
	m.endRebuild(rs)
	return table
}

func (m *FunnelMap[K, V]) beginRebuild(hint mapRebuildHint) *fRebuildState {
	if loadPtr(&m.rs) != nil {
		return nil
	}
	rs := &fRebuildState{hint: hint}
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return nil
	}
	return rs
}

func (m *FunnelMap[K, V]) endRebuild(rs *fRebuildState) {
	atomic.StorePointer(&m.rs, nil)
	rs.latch.Open()
}

//go:noinline
func (m *FunnelMap[K, V]) tryResize(hint mapRebuildHint, newLen uintptr) bool {
	rs := m.beginRebuild(hint)
	if rs == nil {
		return false
	}

	table := (*fMapTable[K, V])(loadPtr(&m.table))
	tableLen := table.mask + 1
	if hint == mapGrowHint {
		if newLen <= tableLen {
			m.endRebuild(rs)
			return true
		}
	} else {
		if newLen >= tableLen || newLen < m.minLen {
			m.endRebuild(rs)
			return true
		}
	}

	rs.table = unsafe.Pointer(table)
	cpus := maxProcs()
	newTable := newFunnelMapTable[K, V](newLen, cpus)
	atomic.StorePointer(&rs.newTable, unsafe.Pointer(newTable))
	m.helpCopyAndWait(rs)
	return true
}

//go:noinline
func (m *FunnelMap[K, V]) helpCopyAndWait(rs *fRebuildState) {
	newTable := (*fMapTable[K, V])(loadPtr(&rs.newTable))
	newLen := newTable.mask + 1
	table := (*fMapTable[K, V])(rs.table)
	oldLen := table.mask + 1
	chunks := table.chunks
	// Determines the concurrent task range for destination buckets.
	// We iterate based on the "Destination Constraint" to allow lock-free
	// writes:
	// - Grow (Pow2):   baseLen == oldLen. Source i moves to Dest i, i+baseLen...
	// - Shrink (Pow2): baseLen == newLen. Source i, i+baseLen... move to Dest i.
	// By iterating 0..baseLen and processing all aliasing source buckets
	// (srcIdx += baseLen) in the inner loop, a single goroutine exclusively
	// owns the write operations for its assigned destination buckets.
	baseLen := min(newLen, oldLen)
	chunkSz := (baseLen + (chunks) - 1) / (chunks)
	for {
		process := uintptr(atomic.AddUint32(&rs.process, 1))
		if process > chunks {
			// Wait copying completed
			rs.latch.Wait()
			return
		}
		process--
		start := (process) * chunkSz
		end := min(start+chunkSz, baseLen)
		m.copyBucket(table, start, end, oldLen, baseLen, newTable)
		if uintptr(atomic.AddUint32(&rs.completed, 1)) == chunks {
			m.copyBucketWithOverflow(table, newTable)
			atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
			m.endRebuild(rs)
			return
		}
	}
}

func (m *FunnelMap[K, V]) copyBucket(
	table *fMapTable[K, V],
	start, end uintptr,
	oldLen, baseLen uintptr,
	newTable *fMapTable[K, V],
) {
	mask := newTable.mask
	var copied uintptr
	for i := start; i < end; i++ {
		// Visit all source buckets that map to this destination bucket.
		// In Grow, runs once. In Shrink, runs twice (usually).
		for srcIdx := i; srcIdx < oldLen; srcIdx += baseLen {
			srcB := table.buckets.At(srcIdx)
			srcB.Lock()
			b := srcB
			meta := loadUint64Fast(&b.meta)
			for marked := meta & fMetaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := (*entry_[K, V])(*b.At(j))
				var hash uintptr
				var h1v uintptr
				if opt.EmbeddedHash_ {
					hash = e.GetHash()
					if m.intKey {
						h1v = hash / fEntriesPerBucket
					} else {
						h1v = h1(hash)
					}
				} else {
					if m.intKey {
						hash = intHash[K](noescape(unsafe.Pointer(&e.key)))
						h1v = hash / fEntriesPerBucket
					} else {
						hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
						h1v = h1(hash)
					}
				}
				idx := mask & h1v
				destB := newTable.buckets.At(idx)
				h2v := h2(hash)
				// Append entry to the destination bucket
				destMeta := destB.meta
				if empty := (^destMeta) & fMetaMask; empty != 0 {
					emptyIdx := firstMarkedByteIndex(empty)
					destB.meta = setByte(destMeta, h2v, emptyIdx)
					*destB.At(emptyIdx) = unsafe.Pointer(e)
					copied++
				} else {
					destB.meta = destMeta | opNextMask
					newTable.overflow.Store(e.key, e.value)
				}
			}
			srcB.Unlock()
		}
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

func (m *FunnelMap[K, V]) copyBucketWithOverflow(table *fMapTable[K, V], newTable *fMapTable[K, V]) {
	// Wait for any in-flight lock-free overflow writers to finish their mutations
	// on the old table. Since all chunks are fully copied, no new writers can pass
	// the resize state check and lock a bucket. Thus, this counter will strictly
	// drain to 0 without deadlocking.
	for atomic.LoadInt32(&table.overflowWriters) > 0 {
		runtime.Gosched()
	}

	// Copying completed
	table.overflow.Range(func(key K, value V) bool {
		var hash uintptr
		var h1v uintptr
		if m.intKey {
			hash = intHash[K](noescape(unsafe.Pointer(&key)))
			h1v = hash / fEntriesPerBucket
		} else {
			hash = m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
			h1v = h1(hash)
		}
		idx := newTable.mask & h1v
		destB := newTable.buckets.At(idx)
		h2v := h2(hash)

		destMeta := destB.meta
		if empty := (^destMeta) & fMetaMask; empty != 0 {
			emptyIdx := firstMarkedByteIndex(empty)
			destB.meta = setByte(destMeta, h2v, emptyIdx)
			newEntry := &entry_[K, V]{key: key, value: value}
			if opt.EmbeddedHash_ {
				newEntry.SetHash(hash)
			}
			*destB.At(emptyIdx) = unsafe.Pointer(newEntry)
			newTable.AddSize(idx, 1)
		} else {
			destB.meta = destMeta | opNextMask
			newTable.overflow.Store(key, value)
		}
		return true
	})
}

// AddSize atomically adds delta to the size counter for the given bucket index.
//
//go:nosplit
func (t *fMapTable[K, V]) AddSize(idx, delta uintptr) {
	t.size.Add(delta)
}

// SumSize calculates the total number of entries in the table
// by summing all counter-stripes.
//
//go:nosplit
func (t *fMapTable[K, V]) SumSize() uintptr {
	return t.size.Value()
}

//go:nosplit
func (b *fBucket) At(i uintptr) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(
		unsafe.Pointer(&b.entries),
		i*unsafe.Sizeof(unsafe.Pointer(nil))),
	)
}

// Lock acquires a spinlock for the bucket using embedded metadata.
// Uses atomic operations on the meta field to avoid false sharing overhead.
// Implements optimistic locking with fallback to spinning.
func (b *fBucket) Lock() {
	// Inline BitLockUint64(&b.meta, opLockMask)
	cur := atomic.LoadUint64(&b.meta)
	if !atomic.CompareAndSwapUint64(&b.meta, cur&^opLockMask, cur|opLockMask) {
		slowBitLockUint64(&b.meta, opLockMask)
	}
}

//go:nosplit
func (b *fBucket) Unlock() {
	BitUnlockUint64(&b.meta, opLockMask)
}

//go:nosplit
func (b *fBucket) UnlockWithMeta(meta uint64) {
	BitUnlockWithStoreUint64(&b.meta, opLockMask, meta)
}

const (
	// fEntriesPerBucket defines the number of per-bucket entry pointers.
	// Computed at compile time to avoid padding while packing buckets
	// tightly within cache lines.
	//
	// Calculation:
	//   ptrSize  = sizeof(unsafe.Pointer)
	//   overhead = 8(meta) + ptrSize(next)
	//   target   = min(CacheLineSize, base)
	//   base     = 32 on 32-bit, 64 on 64-bit
	//   entries  = min(7, (target - overhead) / ptrSize)
	//
	// Rationale:
	//   - 64-bit: bucket size becomes 64B → 1/2/4 buckets per
	//     64/128/256B cache line, with no per-bucket padding.
	//   - 32-bit: bucket size becomes 32B → 2/4/8 buckets per
	//     64/128/256B cache line, also without padding.
	//
	// Example outcomes (cacheLineSize → entriesPerBucket):
	//   64bit: 32B → 3; 64B → 7; 128B → 7; 256B → 7
	//   32bit: 32B → 6; 64B → 6; 128B → 6; 256B → 6
	fBucketOverhead = unsafe.Sizeof(struct {
		meta uint64
	}{})
	fMaxBucketBytes   = min(cacheLineSize, 32+32*(pointerSize/8))
	fEntriesPerBucket = min(7, (fMaxBucketBytes-fBucketOverhead)/pointerSize)

	// Metadata constants for bucket entry management
	fMetaMask uint64 = 0x8080808080808080 >> (64 - min(fEntriesPerBucket*8, 64))
)

//go:nosplit
func fMarkZeroBytes(w uint64) uint64 {
	return (w - 0x0101010101010101) &^ w & fMetaMask
}
