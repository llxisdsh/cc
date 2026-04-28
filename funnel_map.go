package cc

import (
	"cmp"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/llxisdsh/cc/internal/opt"
)

// FunnelMap is a high-throughput, concurrent-safe hash map that leverages a
// SkipMap as its collision resolution mechanism. By combining the cache-friendly
// inline storage of a hash table with the lock-free ordered overflow management
// of a skip list, it retains the fast-path advantages of a traditional Map while
// exhibiting extreme resilience and sustained throughput even under pathological
// hash collision rates. It incorporates a PLocal counter to minimize global
// contention, pushing sequential throughput near hardware limits.
type FunnelMap[K cmp.Ordered, V any] struct {
	_        noCopy
	table    unsafe.Pointer // [*funnelTable]
	intKey   bool
	shrinkOn bool // [WithAutoShrink]
	seed     uintptr
	keyHash  HashFunc
	valEqual EqualFunc
	rs       unsafe.Pointer // [*funnelRebuildState]
	minLen   uintptr        // [WithCapacity]
}

// funnelRebuildState represents the current state of a resizing operation
type funnelRebuildState struct {
	hint      mapRebuildHint
	chunks    uint32  // number of chunks for resizing
	chunkSz   uintptr // size of each chunk for resizing
	latch     Latch
	oldTable  unsafe.Pointer // [*funnelTable]
	newTable  unsafe.Pointer // [*funnelTable]
	process   uint32         // atomic
	completed uint32         // atomic
}

// funnelTable represents the internal hash table structure.
type funnelTable[K cmp.Ordered, V any] struct {
	buckets  unsafeSlice[funnelBucket]
	mask     uintptr
	overflow *SkipMap[K, V]
	size     PLocalCounter // size counts only entries in the buckets array
}

// funnelBucket represents a hash table bucket with cache-line alignment.
type funnelBucket struct {
	// meta: metadata for fast entry lookups, must be 64-bit aligned
	_       [0]atomic.Uint64
	meta    uint64
	entries [fEntriesPerBucket]unsafe.Pointer // [*entry_]
}

// NewFunnelMap creates a new FunnelMap instance. Direct initialization is also
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
) *funnelTable[K, V] {
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
	newLen := fCalcTableLen(cfg.capacity)
	m.minLen = newLen
	m.shrinkOn = cfg.autoShrink
	newTable := newFunnelTable[K, V](newLen)
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	return newTable
}

func newFunnelTable[K cmp.Ordered, V any](tableLen uintptr) *funnelTable[K, V] {
	table := &funnelTable[K, V]{
		buckets:  makeUnsafeSlice[funnelBucket](tableLen),
		mask:     tableLen - 1,
		overflow: NewSkipMap[K, V](),
	}
	return table
}

// Load retrieves a value for the given key.
//
//go:nosplit
func (m *FunnelMap[K, V]) Load(key K) (value V, ok bool) {
	table := (*funnelTable[K, V])(loadPtr(&m.table))
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
			//goland:noinspection GoBoolExpressions
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
	table := (*funnelTable[K, V])(loadPtr(&m.table))
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
				//goland:noinspection GoBoolExpressions
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
		if rs := (*funnelRebuildState)(loadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if loadPtr(&rs.newTable) != nil {
					b.Unlock()
					m.helpCopyAndWait(rs)
					table = (*funnelTable[K, V])(loadPtr(&m.table))
					continue
				}
			case mapRebuildBlockWritersHint:
				b.Unlock()
				rs.latch.Wait()
				table = (*funnelTable[K, V])(loadPtr(&m.table))
				continue
			default:
				// mapRebuildAllowWritersHint: allow concurrent writers
			}
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := (*funnelTable[K, V])(loadPtr(&m.table)); table != newTable {
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
			//goland:noinspection GoBoolExpressions
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
				table.AddSize(1)
				return
			}
		}

		// overflow
		table.overflow.Store(key, value)
		b.UnlockWithMeta(meta | opNextMask)

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
	return m.compute(&key, unsafe.Pointer(&value), computeInit|computeSkipIfFound)
}

// LoadOrStoreFn returns the existing value for the key if
// present. Otherwise, it tries to compute the value using the
// provided function and, if successful, stores and returns
// the computed value. The loaded result is true if the value was
// loaded, or false if computed.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the newValueFn executes. Consider
// this when the function includes long-running operations.
func (m *FunnelMap[K, V]) LoadOrStoreFn(
	key K,
	newValueFn func() V,
) (actual V, loaded bool) {
	fn := func(e *MapEntry[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(newValueFn())
	}
	return m.compute(&key, unsafe.Pointer(&fn), computeInit|computeSkipIfFound|computeUsesFunc)
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
	return m.compute(&key, unsafe.Pointer(&value), computeSkipIfNotFound)
}

// LoadAndDelete retrieves the value for a key and deletes it from the map.
func (m *FunnelMap[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.compute(&key, nil, computeSkipIfNotFound)
}

// Swap stores a key-value pair and returns the previous value if any.
func (m *FunnelMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	return m.compute(&key, unsafe.Pointer(&value), computeInit)
}

// CompareAndSwap atomically replaces an existing value with a new value.
// If the existing value matches the expected value.
func (m *FunnelMap[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	table := (*funnelTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndSwap when value is not of comparable type")
	}
	fn := func(e *MapEntry[K, V]) {
		if e.Loaded() {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.entry.value)),
				noescape(unsafe.Pointer(&old))) {
				e.Update(new)
				swapped = true
			}
		}
	}
	m.compute(&key, unsafe.Pointer(&fn), computeSkipIfNotFound|computeUsesFunc)
	return swapped
}

// CompareAndDelete atomically deletes an existing entry.
// If its value matches the expected value.
func (m *FunnelMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	table := (*funnelTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndDelete when value is not of comparable type")
	}
	fn := func(e *MapEntry[K, V]) {
		if e.Loaded() {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.entry.value)),
				noescape(unsafe.Pointer(&old))) {
				e.Delete()
				deleted = true
			}
		}
	}
	m.compute(&key, unsafe.Pointer(&fn), computeSkipIfNotFound|computeUsesFunc)
	return deleted
}

// Compute performs a compute-style, atomic update for the given key.
//
// Concurrency model:
//   - Acquires the root-bucket lock to serialize write/resize cooperation.
//   - If a resize is observed, cooperates to finish copying and restarts on
//     the latest table.
//
// Callback signature:
//
//		fn(e *MapEntry[K, V])
//
//	  - Use e.Loaded() and e.Value() to inspect the current state
//	  - Use e.Update(newV) to upsert; Use e.Delete() to remove
//
// Parameters:
//
//   - key: The key to process
//   - fn: Callback function (called regardless of value existence)
//
// Returns:
//   - actual: The value as returned by the callback.
//   - loaded: True if the key existed before the callback, false otherwise.
func (m *FunnelMap[K, V]) Compute(
	key K,
	fn func(e *MapEntry[K, V]),
) (actual V, loaded bool) {
	return m.compute(&key, unsafe.Pointer(&fn), computeInit|computeUsesFunc)
}

func (m *FunnelMap[K, V]) compute(
	key *K,
	val unsafe.Pointer, // *V or func(e *MapEntry[K, V])
	flags uint8,
) (actual V, loaded bool) {
	table := (*funnelTable[K, V])(loadPtr(&m.table))
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
				//goland:noinspection GoBoolExpressions
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
			if rs := (*funnelRebuildState)(loadPtr(&m.rs)); rs != nil {
				switch rs.hint {
				case mapGrowHint, mapShrinkHint:
					if loadPtr(&rs.newTable) != nil {
						b.Unlock()
						m.helpCopyAndWait(rs)
						table = (*funnelTable[K, V])(loadPtr(&m.table))
						continue
					}
				case mapRebuildBlockWritersHint:
					b.Unlock()
					rs.latch.Wait()
					table = (*funnelTable[K, V])(loadPtr(&m.table))
					continue
				default:
					// mapRebuildAllowWritersHint: allow concurrent writers
				}
			}
		}

		// Verifies if table was replaced after lock acquisition.
		// Needed since another goroutine may have resized the table
		// between initial check and lock acquisition.
		if newTable := (*funnelTable[K, V])(loadPtr(&m.table)); table != newTable {
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
			//goland:noinspection GoBoolExpressions
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
		if flags&(computeUsesFunc|computeSkipIfFound|computeSkipIfNotFound) == 0 {
			// Store, Swap
			it.entry.value = *(*V)(val)
			it.op = updateOp
		} else if flags&(computeUsesFunc|computeSkipIfFound) == 0 {
			// Delete, LoadAnd.., CompareAnd..
			if it.loaded {
				if val != nil {
					it.entry.value = *(*V)(val)
					it.op = updateOp
				} else {
					it.op = deleteOp
				}
			}
		} else if flags&computeUsesFunc == 0 {
			// LoadOr..
			if !it.loaded {
				it.entry.value = *(*V)(val)
				it.op = updateOp
				retV = it.entry.value
			}
		} else {
			// Compute, LoadOrStoreFn
			(*(*func(e *MapEntry[K, V]))(val))(noEscape(&it))
			retV = it.entry.value
		}

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
					table.overflow.Store(*key, it.entry.value)
					b.Unlock()
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
				table.AddSize(1)
				return retV, it.loaded
			}
			b.UnlockWithMeta(meta | opNextMask)
			table.overflow.Store(*key, it.entry.value)

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
				table.overflow.Delete(*key)
				b.Unlock()
				return retV, it.loaded
			}
			// Delete
			storePtr(b.At(j), nil)
			newMeta := setByte(meta, h2Empty, j)
			b.UnlockWithMeta(newMeta)
			table.AddSize(^uintptr(0))

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
	table := (*funnelTable[K, V])(loadPtr(&m.table))
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
	table := (*funnelTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return 0
	}
	return int(table.SumSize()) + table.overflow.Size()
}

// ToMap collect up to limit entries into a map[K]V, limit < 0 is no limit.
func (m *FunnelMap[K, V]) ToMap(limit ...int) map[K]V {
	l := maxInt
	if len(limit) != 0 {
		l = limit[0]
		if l <= 0 {
			return map[K]V{}
		}
	}

	a := make(map[K]V, min(m.Size(), l))
	for k, v := range m.All() {
		a[k] = v
		l--
		if l == 0 {
			break
		}
	}
	return a
}

// Entries returns an iterator function for use with range-over-func.
// It provides the same functionality as ComputeRange but in iterator form.
//
//go:nosplit
func (m *FunnelMap[K, V]) Entries() func(yield func(e *MapEntry[K, V]) bool) {
	return m.ComputeRange
}

// ComputeRange iterates all entries and applies a user callback.
//
// Callback signature:
//
//		yield(e *MapEntry[K, V]) bool
//
//	  - e.Update(newV): update the entry to newV
//	  - e.Delete(): delete the entry
//	  - default (no op): keep the entry unchanged
//	  - return true to continue; return false to stop iteration
//
// Concurrency & consistency:
//   - Allows concurrent writers during iteration.
//   - Uses per-bucket locking: holds the root-bucket lock while processing
//     its bucket chain to coordinate with writers and resize operations.
//   - Cooperates with concurrent grow/shrink; if a resize is detected, it
//     helps complete copying, then continues on the latest table.
//   - Provides weakly consistent iteration: entries may be updated, added,
//     or removed concurrently, and may or may not be observed.
//
// Parameters:
//   - yield: user function applied to each key-value pair.
//
// Recommendation: keep yield lightweight to reduce lock hold time.
func (m *FunnelMap[K, V]) ComputeRange(yield func(e *MapEntry[K, V]) bool) {
	m.computeRange(yield, false)
}

func (m *FunnelMap[K, V]) computeRange(yield func(e *MapEntry[K, V]) bool, ignoreRebuildState bool) {
restart:
	table := (*funnelTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return
	}
	it := MapEntry[K, V]{
		loaded: true,
	}

	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
		b.Lock()

		if !ignoreRebuildState {
			if rs := (*funnelRebuildState)(loadPtr(&m.rs)); rs != nil {
				switch rs.hint {
				case mapGrowHint, mapShrinkHint:
					if loadPtr(&rs.newTable) != nil {
						b.Unlock()
						m.helpCopyAndWait(rs)
						goto restart
					}
				case mapRebuildBlockWritersHint:
					b.Unlock()
					rs.latch.Wait()
					goto restart
				default:
					// mapRebuildAllowWritersHint: allow concurrent writers
				}
			}

			if newTable := (*funnelTable[K, V])(loadPtr(&m.table)); table != newTable {
				b.Unlock()
				goto restart
			}
		}

		meta := loadUint64Fast(&b.meta)
		for marked := meta & fMetaMask; marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			e := (*entry_[K, V])(*b.At(j))
			it.entry = *e
			it.op = cancelOp
			shouldContinue := yield(noEscape(&it))

			switch it.op {
			case updateOp:
				newEntry := &entry_[K, V]{key: e.key, value: it.entry.value}
				if opt.EmbeddedHash_ {
					newEntry.SetHash(e.GetHash())
				}
				storePtr(b.At(j), unsafe.Pointer(newEntry))
			case deleteOp:
				storePtr(b.At(j), nil)
				meta = setByte(meta, h2Empty, j)
				storeUint64(&b.meta, meta)
				table.AddSize(^uintptr(0))
			default:
				// cancelOp: no-op
			}

			if !shouldContinue {
				b.Unlock()
				return
			}
		}
		b.Unlock()
	}

	// Process overflow
	overflow := table.overflow
	for k, v := range overflow.All() {
		if !ignoreRebuildState {
			if rs := (*funnelRebuildState)(loadPtr(&m.rs)); rs != nil {
				switch rs.hint {
				case mapGrowHint, mapShrinkHint:
					if loadPtr(&rs.newTable) != nil {
						m.helpCopyAndWait(rs)
						goto restart
					}
				case mapRebuildBlockWritersHint:
					rs.latch.Wait()
					goto restart
				default:
					// mapRebuildAllowWritersHint: allow concurrent writers
				}
			}

			if newTable := (*funnelTable[K, V])(loadPtr(&m.table)); table != newTable {
				goto restart
			}
		}

		it.entry.key = k
		it.entry.value = v
		it.op = cancelOp
		shouldContinue := yield(noEscape(&it))
		switch it.op {
		case updateOp:
			overflow.Store(k, it.entry.value)
		case deleteOp:
			overflow.Delete(k)
		default:
			// cancelOp: no-op
		}
		if !shouldContinue {
			return
		}
	}
}

// Clear clears all key-value pairs from the map.
func (m *FunnelMap[K, V]) Clear() {
	table := (*funnelTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return
	}
	m.rebuild(mapRebuildBlockWritersHint, func() {
		newTable := newFunnelTable[K, V](m.minLen)
		atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	})
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
	table := (*funnelTable[K, V])(loadPtr(&m.table))
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
		table := (*funnelTable[K, V])(loadPtr(&m.table))
		tableLen := table.mask + 1
		var newLen uintptr
		if hint == mapGrowHint {
			if sizeAdd <= 0 {
				return
			}
			newLen = fCalcTableLen(table.SumSize() + uintptr(table.overflow.Size()) + sizeAdd)
			if newLen <= tableLen {
				return
			}
		} else {
			// mapShrinkHint
			if tableLen <= m.minLen {
				return
			}
			newLen = fCalcTableLen(table.SumSize() + uintptr(table.overflow.Size()))
			if newLen >= tableLen {
				return
			}
		}
		// Help finishing rebuild if needed
		if rs := (*funnelRebuildState)(loadPtr(&m.rs)); rs != nil {
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

// CloneTo copies all key-value pairs from this map to the destination map.
// The destination map is cleared before copying.
//
// Parameters:
//   - clone: The destination map to copy into. Must not be nil.
//
// Notes:
//   - This operation is not atomic with respect to concurrent modifications.
//   - The destination map will have the same configuration as the source.
//   - The destination map is cleared before copying to ensure a clean state.
func (m *FunnelMap[K, V]) CloneTo(clone *FunnelMap[K, V]) {
	clone.Clear()
	table := (*funnelTable[K, V])(loadPtr(&m.table))
	if table == nil {
		return
	}

	clone.seed = m.seed
	clone.keyHash = m.keyHash
	clone.valEqual = m.valEqual
	clone.minLen = m.minLen
	clone.shrinkOn = m.shrinkOn
	clone.intKey = m.intKey
	newLen := fCalcTableLen(table.SumSize())
	newTable := newFunnelTable[K, V](newLen)
	atomic.StorePointer(&clone.table, unsafe.Pointer(newTable))
	for k, v := range m.All() {
		clone.Store(k, v)
	}
}

// rebuild reorganizes the map. Only these hints are supported:
//   - mapRebuildAllowWritersHint: allows concurrent reads/writes
//   - mapRebuildBlockWritersHint: allows concurrent reads
func (m *FunnelMap[K, V]) rebuild(
	hint mapRebuildHint,
	fn func(),
) {
	for {
		// Help finishing rebuild if needed
		if rs := (*funnelRebuildState)(loadPtr(&m.rs)); rs != nil {
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

		if rs := m.beginRebuild(hint); rs != nil {
			fn()
			m.endRebuild(rs)
			return
		}
	}
}

// slowInit may be called concurrently by multiple goroutines, so it requires
// synchronization with a "lock" mechanism.
//
//go:noinline
func (m *FunnelMap[K, V]) slowInit() *funnelTable[K, V] {
	rs := m.beginRebuild(mapRebuildBlockWritersHint)
	if rs == nil {
		// Another goroutine is initializing, wait for it to complete
		rs = (*funnelRebuildState)(loadPtr(&m.rs))
		if rs != nil {
			rs.latch.Wait()
		}
		// Now the table should be initialized
		return (*funnelTable[K, V])(loadPtr(&m.table))
	}

	// Although the table is always changed when rs is not nil,
	// it might have been changed before that.
	table := (*funnelTable[K, V])(loadPtr(&m.table))
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

func (m *FunnelMap[K, V]) beginRebuild(hint mapRebuildHint) *funnelRebuildState {
	if loadPtr(&m.rs) != nil {
		return nil
	}
	rs := &funnelRebuildState{hint: hint}
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return nil
	}
	return rs
}

func (m *FunnelMap[K, V]) endRebuild(rs *funnelRebuildState) {
	atomic.StorePointer(&m.rs, nil)
	rs.latch.Open()
}

//go:noinline
func (m *FunnelMap[K, V]) tryResize(hint mapRebuildHint, newLen uintptr) bool {
	rs := m.beginRebuild(hint)
	if rs == nil {
		return false
	}

	table := (*funnelTable[K, V])(loadPtr(&m.table))
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

	cpus := maxProcs()
	chunks := uint32(calcParallelism(tableLen, cpus*resizeOverPartition))
	// Determines the concurrent task range for destination buckets.
	// We iterate based on the "Destination Constraint" to allow lock-free
	// writes:
	// - Grow (Pow2):   baseLen == oldLen. Source i moves to Dest i, i+baseLen...
	// - Shrink (Pow2): baseLen == newLen. Source i, i+baseLen... move to Dest i.
	// By iterating 0..baseLen and processing all aliasing source buckets
	// (srcIdx += baseLen) in the inner loop, a single goroutine exclusively
	// owns the write operations for its assigned destination buckets.
	baseLen := min(newLen, tableLen)
	chunkSz := max(1, baseLen>>bits.TrailingZeros32(chunks))
	rs.chunks = chunks
	rs.chunkSz = chunkSz
	rs.oldTable = unsafe.Pointer(table)
	newTable := newFunnelTable[K, V](newLen)
	atomic.StorePointer(&rs.newTable, unsafe.Pointer(newTable))
	m.helpCopyAndWait(rs)
	return true
}

//go:noinline
func (m *FunnelMap[K, V]) helpCopyAndWait(rs *funnelRebuildState) {
	newTable := (*funnelTable[K, V])(loadPtr(&rs.newTable))
	newLen := newTable.mask + 1
	oldTable := (*funnelTable[K, V])(rs.oldTable)
	oldLen := oldTable.mask + 1
	chunks := rs.chunks
	chunkSz := rs.chunkSz
	baseLen := min(newLen, oldLen)
	for {
		process := atomic.AddUint32(&rs.process, 1)
		if process > chunks {
			// Wait copying completed
			rs.latch.Wait()
			return
		}
		process--
		start := uintptr(process) * chunkSz
		end := min(start+chunkSz, baseLen)
		m.copyBucket(oldTable, start, end, oldLen, baseLen, newTable)
		if atomic.AddUint32(&rs.completed, 1) == chunks {
			m.copyBucketWithOverflow(oldTable, newTable)
			atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
			m.endRebuild(rs)
			return
		}
	}
}

func (m *FunnelMap[K, V]) copyBucket(
	table *funnelTable[K, V],
	start, end uintptr,
	oldLen, baseLen uintptr,
	newTable *funnelTable[K, V],
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
		newTable.AddSize(copied)
	}
}

func (m *FunnelMap[K, V]) copyBucketWithOverflow(table *funnelTable[K, V], newTable *funnelTable[K, V]) {
	var copied uintptr
	for k, v := range table.overflow.All() {
		var hash uintptr
		var h1v uintptr
		if m.intKey {
			hash = intHash[K](noescape(unsafe.Pointer(&k)))
			h1v = hash / fEntriesPerBucket
		} else {
			hash = m.keyHash(noescape(unsafe.Pointer(&k)), m.seed)
			h1v = h1(hash)
		}
		idx := newTable.mask & h1v
		destB := newTable.buckets.At(idx)
		h2v := h2(hash)

		destMeta := destB.meta
		if empty := (^destMeta) & fMetaMask; empty != 0 {
			emptyIdx := firstMarkedByteIndex(empty)
			destB.meta = setByte(destMeta, h2v, emptyIdx)
			newEntry := &entry_[K, V]{key: k, value: v}
			if opt.EmbeddedHash_ {
				newEntry.SetHash(hash)
			}
			*destB.At(emptyIdx) = unsafe.Pointer(newEntry)
			copied++
		} else {
			destB.meta = destMeta | opNextMask
			newTable.overflow.Store(k, v)
		}
	}
	if copied != 0 {
		newTable.AddSize(copied)
	}
}

// AddSize atomically adds delta to the size counter for the given bucket index.
//
//go:nosplit
func (t *funnelTable[K, V]) AddSize(delta uintptr) {
	t.size.Add(delta)
}

// SumSize calculates the total number of entries in the table
// by summing all counter-stripes.
//
//go:nosplit
func (t *funnelTable[K, V]) SumSize() uintptr {
	return t.size.Value()
}

//go:nosplit
func (b *funnelBucket) At(i uintptr) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(
		unsafe.Pointer(&b.entries),
		i*unsafe.Sizeof(unsafe.Pointer(nil))),
	)
}

// Lock acquires a spinlock for the bucket using embedded metadata.
// Uses atomic operations on the meta field to avoid false sharing overhead.
// Implements optimistic locking with fallback to spinning.
func (b *funnelBucket) Lock() {
	// Inline BitLockUint64(&b.meta, opLockMask)
	cur := atomic.LoadUint64(&b.meta)
	if !atomic.CompareAndSwapUint64(&b.meta, cur&^opLockMask, cur|opLockMask) {
		slowBitLockUint64(&b.meta, opLockMask)
	}
}

//go:nosplit
func (b *funnelBucket) Unlock() {
	BitUnlockUint64(&b.meta, opLockMask)
}

//go:nosplit
func (b *funnelBucket) UnlockWithMeta(meta uint64) {
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

//go:nosplit
func fCalcTableLen(capacity uintptr) uintptr {
	tableLen := uintptr(minTableLen)
	const minThreshold = uintptr(float64(minTableLen*fEntriesPerBucket) * loadFactor)
	if capacity >= minThreshold {
		const invFactor = 1.0 / (float64(fEntriesPerBucket) * loadFactor)
		// +entriesPerBucket-1 is used to compensate for calculation
		// inaccuracies
		tableLen = nextPowOf2(
			uintptr(float64(capacity+fEntriesPerBucket-1) * invFactor),
		)
	}
	return tableLen
}
