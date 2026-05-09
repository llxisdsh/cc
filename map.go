package cc

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/llxisdsh/cc/internal/opt"
)

// Map is a high-performance concurrent map implementation that is fully
// compatible with sync.Map API and significantly outperforms sync.Map in
// most scenarios.
//
// Core advantages:
//   - Lock-free reads, fine-grained locking for writes
//   - Zero-value ready with lazy initialization
//   - Custom hash and value comparison function support
//   - Rich batch operations and functional extensions
//
// Usage recommendations:
//   - Direct declaration: var m Map[string, int]
//   - Pre-allocate capacity: NewMap(WithCapacity(1000))
//
// Notes:
//   - Map must not be copied after first use.
type Map[K comparable, V any] struct {
	_        noCopy
	table    unsafe.Pointer // [*mapTable]
	intKey   bool
	shrinkOn bool // [WithAutoShrink]
	seed     uintptr
	keyHash  HashFunc
	valEqual EqualFunc
	rs       unsafe.Pointer // [*rebuildState]
	minLen   uintptr        // [WithCapacity]
	growths  uint32
	shrinks  uint32
}

// rebuildState represents the current state of a resizing operation
type rebuildState struct {
	hint      mapRebuildHint
	chunks    uint32  // number of chunks for resizing
	chunkSz   uintptr // size of each chunk for resizing
	latch     Latch
	oldTable  unsafe.Pointer // [*mapTable]
	newTable  unsafe.Pointer // [*mapTable]
	process   uint32         // atomic
	completed uint32         // atomic
}

// mapTable represents the internal hash table structure.
type mapTable struct {
	buckets   unsafeSlice[bucket]
	mask      uintptr
	size      unsafeSlice[counterStripe]
	sizeMask  uintptr
	stripeCap int
	growCap   uintptr
}

// bucket represents a hash table bucket with cache-line alignment.
type bucket struct {
	// meta: metadata for fast entry lookups, must be 64-bit aligned
	_       [0]atomic.Uint64
	meta    uint64
	entries [entriesPerBucket]unsafe.Pointer // [*entry_]
	next    unsafe.Pointer                   // [*bucket]
}

// NewMap creates a new Map instance. Direct initialization is also
// supported.
//
// Parameters:
//   - options: configuration options (WithCapacity, WithKeyHasher, etc.)
func NewMap[K comparable, V any](
	options ...func(*MapConfig),
) *Map[K, V] {
	m := &Map[K, V]{}
	m.withOptions(options...)
	return m
}

// withOptions initializes the Map instance using variadic option
// parameters. This is a convenience method that allows configuring Map
// through the functional options pattern.
//
// Configuration Priority (highest to lowest):
//   - Explicit With* functions (WithKeyHasher, WithValueEqual)
//   - Interface implementations (IHashFunc, IEqualFunc)
//   - Default built-in implementations (defaultHasher) - fallback
//
// Parameters:
//   - options: configuration option functions such as WithCapacity,
//     WithAutoShrink, WithKeyHasher, WithValueEqual, etc.
//
// Usage example:
//
//	m.withOptions(WithCapacity(1000), WithAutoShrink())
//
// Notes:
//   - This function is not thread-safe and should only be called before Map
//     is used
//   - If this function is not called, Map will use default configuration
//   - The behavior of calling this function multiple times is undefined
func (m *Map[K, V]) withOptions(
	options ...func(*MapConfig),
) {
	var cfg MapConfig

	// parse options
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m.init(noEscape(&cfg))
}

func (m *Map[K, V]) init(
	cfg *MapConfig,
) *mapTable {
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
	newLen := calcTableLen(cfg.capacity)
	m.minLen = newLen
	m.shrinkOn = cfg.autoShrink
	newTable := newMapTable(newLen, maxProcs())
	atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
	return newTable
}

func newMapTable(tableLen, cpus uintptr) *mapTable {
	sizeLen := calcSizeLen(tableLen, cpus)
	const capFactor = float64(entriesPerBucket) * loadFactor
	growCap := uintptr(float64(tableLen) * capFactor)
	return &mapTable{
		buckets:   makeUnsafeSlice[bucket](tableLen),
		mask:      tableLen - 1,
		size:      makeUnsafeSlice[counterStripe](sizeLen),
		sizeMask:  sizeLen - 1,
		stripeCap: int(growCap >> bits.TrailingZeros32(uint32(sizeLen))),
		growCap:   growCap,
	}
}

// Load retrieves a value for the given key, compatible with `sync.Map`.
//
//go:nosplit
func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return *new(V), false
	}

	var hash uintptr
	var h1v uintptr
	var h2v uint8
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(&key)))
		h1v = hash / entriesPerBucket
		h2v = h2(hash ^ (hash >> 16))
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h1v = h1(hash)
		h2v = h2(hash)
	}
	h2w := broadcast(h2v)
	idx := table.mask & h1v
	for b := table.buckets.At(idx); ; b = (*bucket)(loadPtr(&b.next)) {
		meta := loadUint64(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
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
		if meta&opNextMask == 0 {
			return *new(V), false
		}
	}
}

// Store inserts or updates a key-value pair, compatible with `sync.Map`.
func (m *Map[K, V]) Store(key K, value V) {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		table = m.slowInit()
	}

	var hash uintptr
	var h1v uintptr
	var h2v uint8
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(&key)))
		h1v = hash / entriesPerBucket
		h2v = h2(hash ^ (hash >> 16))
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h1v = h1(hash)
		h2v = h2(hash)
	}
	h2w := broadcast(h2v)
	idx := table.mask & h1v
	root := table.buckets.At(idx)

	// Fast path: lock-free read
	for b := root; ; b = (*bucket)(loadPtr(&b.next)) {
		meta := loadUint64(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if e := (*entry_[K, V])(loadPtr(b.At(j))); e != nil {
				//goland:noinspection GoBoolExpressions
				if !opt.EmbeddedHash_ || e.GetHash() == hash {
					if e.key == key {
						if m.valEqual != nil && m.valEqual(
							noescape(unsafe.Pointer(&e.value)),
							noescape(unsafe.Pointer(&value)),
						) {
							return
						}
						goto slowPath
					}
				}
			}
		}
		if meta&opNextMask == 0 {
			break
		}
	}

slowPath:
	root.Lock()

	// This is the first check, checking if there is a rebuild operation in
	// progress after acquiring the bucket lock
	if rs := (*rebuildState)(loadPtr(&m.rs)); rs != nil {
		switch rs.hint {
		case mapGrowHint, mapShrinkHint:
			if loadPtr(&rs.newTable) != nil {
				root.Unlock()
				m.helpCopyAndWait(rs)
				table = (*mapTable)(loadPtr(&m.table))
				idx = table.mask & h1v
				root = table.buckets.At(idx)
				goto slowPath
			}
		case mapRebuildBlockWritersHint:
			root.Unlock()
			rs.latch.Wait()
			table = (*mapTable)(loadPtr(&m.table))
			idx = table.mask & h1v
			root = table.buckets.At(idx)
			goto slowPath
		default:
			// mapRebuildAllowWritersHint: allow concurrent writers
		}
	}

	// Verifies if table was replaced after lock acquisition.
	// Needed since another goroutine may have resized the table
	// between initial check and lock acquisition.
	if newTable := (*mapTable)(loadPtr(&m.table)); table != newTable {
		root.Unlock()
		table = newTable
		idx = table.mask & h1v
		root = table.buckets.At(idx)
		goto slowPath
	}

	var (
		emptyM uint64
		emptyB *bucket
		emptyI uintptr
	)
	newEntry := &entry_[K, V]{key: key, value: value}
	if opt.EmbeddedHash_ {
		newEntry.SetHash(hash)
	}
	lastB := root
	for {
		meta := loadUint64Fast(&lastB.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			e := (*entry_[K, V])(*lastB.At(j))
			//goland:noinspection GoBoolExpressions
			if !opt.EmbeddedHash_ || e.GetHash() == hash {
				if e.key == key {
					// Update
					storePtr(lastB.At(j), unsafe.Pointer(newEntry))
					root.Unlock()
					return
				}
			}
		}
		if emptyB == nil {
			if empty := (^meta) & metaMask; empty != 0 {
				emptyM, emptyB, emptyI = meta, lastB, firstMarkedByteIndex(empty)
			}
		}
		if meta&opNextMask == 0 {
			break
		}
		lastB = (*bucket)(lastB.next)
	}

	if emptyB == nil {
		// No empty slot found, create new bucket and insert
		storePtr(&lastB.next, unsafe.Pointer(&bucket{
			meta: setByte(metaEmpty, h2v, 0),
			entries: [entriesPerBucket]unsafe.Pointer{
				unsafe.Pointer(newEntry),
			},
		}))
		if lastB == root {
			root.UnlockWithMeta(loadUint64Fast(&lastB.meta) | opNextMask)
		} else {
			storeUint64(&lastB.meta, loadUint64Fast(&lastB.meta)|opNextMask)
			root.Unlock()
		}

		// localSize := int(table.AddSize(idx, 1))
		// Check if the table needs to grow
		if int(table.AddSize(idx, 1)) >= table.stripeCap {
			if loadPtr(&m.rs) == nil {
				if table.SumSize() >= table.growCap {
					m.tryResize(mapGrowHint, (table.mask+1)<<1)
				}
			}
		}
		return
	}

	// Insert into empty slot
	// publish pointer first, then meta; readers check meta before
	// pointer so they won't observe a partially-initialized entry,
	// and this reduces the window where meta is visible but pointer is
	// still nil
	storePtr(emptyB.At(emptyI), unsafe.Pointer(newEntry))
	newMeta := setByte(emptyM, h2v, emptyI)
	if emptyB == root {
		root.UnlockWithMeta(newMeta)
	} else {
		storeUint64(&emptyB.meta, newMeta)
		root.Unlock()
	}
	table.AddSize(idx, 1)
}

// LoadOrStore retrieves an existing value or stores a new one if the key
// doesn't exist, compatible with `sync.Map`.
func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(value)
	}, computeInit|computeSkipIfFound)
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
func (m *Map[K, V]) LoadOrStoreFn(
	key K,
	newValueFn func() V,
) (actual V, loaded bool) {
	return m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(newValueFn())
	}, computeInit|computeSkipIfFound)
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
func (m *Map[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	_, loaded = m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			previous = e.Value()
			e.Update(value)
		}
	}, computeSkipIfNotFound)
	return previous, loaded
}

// LoadAndDelete retrieves the value for a key and deletes it from the map.
// compatible with `sync.Map`.
func (m *Map[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	_, loaded = m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			previous = e.Value()
			e.Delete()
		}
	}, computeSkipIfNotFound)
	return previous, loaded
}

// Swap stores a key-value pair and returns the previous value if any.
// compatible with `sync.Map`.
func (m *Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	_, loaded = m.compute(&key, func(e *MapEntry[K, V]) {
		previous = e.Value()
		e.Update(value)
	}, computeInit)
	return previous, loaded
}

// Delete removes a key-value pair.
// compatible with `sync.Map`.
func (m *Map[K, V]) Delete(key K) {
	m.compute(&key, func(e *MapEntry[K, V]) {
		e.Delete()
	}, computeSkipIfNotFound)
}

// CompareAndSwap atomically replaces an existing value with a new value.
// If the existing value matches the expected value, compatible with `sync.Map`.
func (m *Map[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndSwap when value is not of comparable type")
	}
	m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.entry.value)),
				noescape(unsafe.Pointer(&old)),
			) {
				e.Update(new)
				swapped = true
			}
		}
	}, computeSkipIfNotFound)
	return swapped
}

// CompareAndDelete atomically deletes an existing entry.
// If its value matches the expected value, compatible with `sync.Map`.
func (m *Map[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndDelete when value is not of comparable type")
	}
	m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.entry.value)),
				noescape(unsafe.Pointer(&old)),
			) {
				e.Delete()
				deleted = true
			}
		}
	}, computeSkipIfNotFound)
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
func (m *Map[K, V]) Compute(
	key K,
	fn func(e *MapEntry[K, V]),
) (actual V, loaded bool) {
	return m.compute(&key, fn, computeInit)
}

func (m *Map[K, V]) compute(
	key *K,
	fn func(e *MapEntry[K, V]),
	flags uint8,
) (actual V, loaded bool) {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		if flags&computeInit == 0 {
			return *new(V), false
		}
		table = m.slowInit()
	}

	var hash uintptr
	var h1v uintptr
	var h2v uint8
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(key)))
		h1v = hash / entriesPerBucket
		h2v = h2(hash ^ (hash >> 16))
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
		h1v = h1(hash)
		h2v = h2(hash)
	}
	h2w := broadcast(h2v)
	idx := table.mask & h1v
	root := table.buckets.At(idx)

	// Fast path: lock-free read
	if flags&(computeSkipIfFound|computeSkipIfNotFound) != 0 {
		b := root
		for {
			meta := loadUint64(&b.meta)
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
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
			if meta&opNextMask == 0 {
				break
			}
			b = (*bucket)(loadPtr(&b.next))
		}
		// Key not found in fast path
		if flags&computeSkipIfNotFound != 0 {
			return *new(V), false
		}
	}

slowPath:
	root.Lock()

	// This is the first check, checking if there is a rebuild operation in
	// progress after acquiring the bucket lock
	if flags&computeIgnoreHint == 0 {
		if rs := (*rebuildState)(loadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if loadPtr(&rs.newTable) != nil {
					root.Unlock()
					m.helpCopyAndWait(rs)
					table = (*mapTable)(loadPtr(&m.table))
					idx = table.mask & h1v
					root = table.buckets.At(idx)
					goto slowPath
				}
			case mapRebuildBlockWritersHint:
				root.Unlock()
				rs.latch.Wait()
				table = (*mapTable)(loadPtr(&m.table))
				idx = table.mask & h1v
				root = table.buckets.At(idx)
				goto slowPath
			default:
				// mapRebuildAllowWritersHint: allow concurrent writers
			}
		}
	}

	// Verifies if table was replaced after lock acquisition.
	// Needed since another goroutine may have resized the table
	// between initial check and lock acquisition.
	if newTable := (*mapTable)(loadPtr(&m.table)); table != newTable {
		root.Unlock()
		table = newTable
		idx = table.mask & h1v
		root = table.buckets.At(idx)
		goto slowPath
	}

	var (
		meta   uint64
		j      uintptr
		emptyM uint64
		emptyB *bucket
		emptyI uintptr
	)
	it := MapEntry[K, V]{entry: entry_[K, V]{key: *key}}
	lastB := root
findLoop:
	for {
		meta = loadUint64Fast(&lastB.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j = firstMarkedByteIndex(marked)
			e := (*entry_[K, V])(*lastB.At(j))
			//goland:noinspection GoBoolExpressions
			if !opt.EmbeddedHash_ || e.GetHash() == hash {
				if e.key == *key {
					it.entry.value, it.loaded = e.value, true
					break findLoop
				}
			}
		}
		if emptyB == nil {
			if empty := (^meta) & metaMask; empty != 0 {
				emptyM, emptyB, emptyI = meta, lastB, firstMarkedByteIndex(empty)
			}
		}
		if meta&opNextMask == 0 {
			break
		}
		lastB = (*bucket)(lastB.next)
	}

	// --- Compute Logic ---
	fn(noEscape(&it))

	switch it.op {
	case updateOp:
		if it.loaded {
			// valEqual: skip write if value unchanged
			if m.valEqual != nil && m.valEqual(
				noescape(unsafe.Pointer(&(*entry_[K, V])(*lastB.At(j)).value)),
				noescape(unsafe.Pointer(&it.entry.value)),
			) {
				root.Unlock()
				return it.entry.value, it.loaded
			}
		}

		// Update
		newEntry := &entry_[K, V]{key: *key, value: it.entry.value}
		if opt.EmbeddedHash_ {
			newEntry.SetHash(hash)
		}
		if it.loaded {
			storePtr(lastB.At(j), unsafe.Pointer(newEntry))
			root.Unlock()
			return it.entry.value, it.loaded
		}
		if emptyB == nil {
			// No empty slot, create new bucket and insert
			storePtr(&lastB.next, unsafe.Pointer(&bucket{
				meta: setByte(metaEmpty, h2v, 0),
				entries: [entriesPerBucket]unsafe.Pointer{
					unsafe.Pointer(newEntry),
				},
			}))
			if lastB == root {
				root.UnlockWithMeta(meta | opNextMask)
			} else {
				storeUint64(&lastB.meta, meta|opNextMask)
				root.Unlock()
			}

			localSize := int(table.AddSize(idx, 1))
			// Check if the table needs to grow
			if localSize >= table.stripeCap {
				if loadPtr(&m.rs) == nil {
					if table.SumSize() >= table.growCap {
						m.tryResize(mapGrowHint, (table.mask+1)<<1)
					}
				}
			}
			return it.entry.value, it.loaded
		}
		// Insert into empty slot
		storePtr(emptyB.At(emptyI), unsafe.Pointer(newEntry))
		newMeta := setByte(emptyM, h2v, emptyI)
		if emptyB == root {
			root.UnlockWithMeta(newMeta)
		} else {
			storeUint64(&emptyB.meta, newMeta)
			root.Unlock()
		}
		table.AddSize(idx, 1)
		return it.entry.value, it.loaded
	case deleteOp:
		if !it.loaded {
			root.Unlock()
			return it.entry.value, it.loaded
		}

		// Delete
		storePtr(lastB.At(j), nil)
		newMeta := setByte(meta, h2Empty, j)
		if lastB == root {
			root.UnlockWithMeta(newMeta)
		} else {
			storeUint64(&lastB.meta, newMeta)
			root.Unlock()
		}
		table.AddSize(idx, ^uintptr(0))

		// Check if table shrinking is needed
		if m.shrinkOn {
			if newMeta&metaDataMask == metaEmpty {
				if loadPtr(&m.rs) == nil {
					tableLen := table.mask + 1
					if m.minLen < tableLen {
						size := table.SumSize()
						if size < tableLen*entriesPerBucket/shrinkFraction {
							m.tryResize(mapShrinkHint, tableLen>>1)
						}
					}
				}
			}
		}
		return it.entry.value, it.loaded
	default:
		// cancelOp: no-op
		root.Unlock()
		return it.entry.value, it.loaded
	}
}

// Range compatible with `sync.Map`.
// Notes:
//   - The iteration directly traverses bucket data. The data is not guaranteed
//     to be real-time but provides eventual consistency.
//     In extreme cases, the same value may be traversed twice
//     (if it gets deleted and re-added later during iteration).
func (m *Map[K, V]) Range(yield func(key K, value V) bool) {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return
	}
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
		for {
			meta := loadUint64(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				if e := (*entry_[K, V])(loadPtr(b.At(j))); e != nil {
					if !yield(e.key, e.value) {
						return
					}
				}
			}
			if meta&opNextMask == 0 {
				break
			}
			b = (*bucket)(loadPtr(&b.next))
		}
	}
}

// All compatible with `sync.Map`.
//
//go:nosplit
func (m *Map[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the number of key-value pairs in the map.
// This is an O(1) operation.
//
//go:nosplit
func (m *Map[K, V]) Size() int {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return 0
	}
	return int(table.SumSize())
}

// ToMap collect up to limit entries into a map[K]V, limit < 0 is no limit.
func (m *Map[K, V]) ToMap(limit ...int) map[K]V {
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
func (m *Map[K, V]) Entries() func(yield func(e *MapEntry[K, V]) bool) {
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
func (m *Map[K, V]) ComputeRange(yield func(e *MapEntry[K, V]) bool) {
	m.computeRange(yield, false)
}

func (m *Map[K, V]) computeRange(yield func(e *MapEntry[K, V]) bool, ignoreRebuildState bool) {
restart:
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return
	}
	it := MapEntry[K, V]{
		loaded: true,
	}

	for i := uintptr(0); i <= table.mask; i++ {
		root := table.buckets.At(i)
		root.Lock()

		if !ignoreRebuildState {
			if rs := (*rebuildState)(loadPtr(&m.rs)); rs != nil {
				switch rs.hint {
				case mapGrowHint, mapShrinkHint:
					if loadPtr(&rs.newTable) != nil {
						root.Unlock()
						m.helpCopyAndWait(rs)
						goto restart
					}
				case mapRebuildBlockWritersHint:
					root.Unlock()
					rs.latch.Wait()
					goto restart
				default:
					// mapRebuildAllowWritersHint: allow concurrent writers
				}
			}

			if newTable := (*mapTable)(loadPtr(&m.table)); table != newTable {
				root.Unlock()
				goto restart
			}
		}

		b := root
		for {
			meta := loadUint64Fast(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
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
					table.AddSize(i, ^uintptr(0))
				default:
					// cancelOp: no-op
				}

				if !shouldContinue {
					root.Unlock()
					return
				}
			}
			if meta&opNextMask == 0 {
				break
			}
			b = (*bucket)(b.next)
		}
		root.Unlock()
	}
}

// Clear compatible with `sync.Map`
func (m *Map[K, V]) Clear() {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return
	}
	m.rebuild(mapRebuildBlockWritersHint, func(_ *MapRebuild[K, V]) {
		newTable := newMapTable(m.minLen, maxProcs())
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
func (m *Map[K, V]) Grow(sizeAdd int) {
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
func (m *Map[K, V]) Shrink() {
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return
	}
	m.doResize(mapShrinkHint, 0)
}

func (m *Map[K, V]) doResize(
	hint mapRebuildHint,
	sizeAdd uintptr,
) {
	for {
		// Resize check
		table := (*mapTable)(loadPtr(&m.table))
		tableLen := table.mask + 1
		var newLen uintptr
		if hint == mapGrowHint {
			if sizeAdd <= 0 {
				return
			}
			newLen = calcTableLen(table.SumSize() + sizeAdd)
			if newLen <= tableLen {
				return
			}
		} else {
			// mapShrinkHint
			if tableLen <= m.minLen {
				return
			}
			newLen = calcTableLen(table.SumSize())
			if newLen >= tableLen {
				return
			}
		}
		// Help finishing rebuild if needed
		if rs := (*rebuildState)(loadPtr(&m.rs)); rs != nil {
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
func (m *Map[K, V]) CloneTo(clone *Map[K, V]) {
	clone.Clear()
	table := (*mapTable)(loadPtr(&m.table))
	if table == nil {
		return
	}
	clone.intKey = m.intKey
	clone.shrinkOn = m.shrinkOn
	clone.seed = m.seed
	clone.keyHash = m.keyHash
	clone.valEqual = m.valEqual
	clone.minLen = m.minLen
	newLen := calcTableLen(table.SumSize())
	newTable := newMapTable(newLen, maxProcs())
	atomic.StorePointer(&clone.table, unsafe.Pointer(newTable))
	for k, v := range m.All() {
		clone.Store(k, v)
	}
}

// Rebuild performs a map rebuild operation with the given function.
// The function is executed with exclusive access to the map.
// Concurrent writers are always blocked during the rebuild.
//
// Parameters:
//   - fn: The function to execute during rebuild.
//     It receives a MapRebuild instance.
//
// Notes:
//   - You must use the `m *MapRebuild[K, V]` parameter passed to `fn` for
//     processing. Do not call methods on the Map instance directly, as this
//     may cause deadlocks.
func (m *Map[K, V]) Rebuild(fn func(m *MapRebuild[K, V])) {
	m.rebuild(mapRebuildBlockWritersHint, fn)
}

// rebuild reorganizes the map. Only these hints are supported:
//   - mapRebuildAllowWritersHint: allows concurrent reads/writes
//   - mapRebuildBlockWritersHint: allows concurrent reads
func (m *Map[K, V]) rebuild(
	hint mapRebuildHint,
	fn func(m *MapRebuild[K, V]),
) {
	for {
		// Help finishing rebuild if needed
		if rs := (*rebuildState)(loadPtr(&m.rs)); rs != nil {
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
			fn(noEscape(&MapRebuild[K, V]{m: m}))
			m.endRebuild(rs)
			return
		}
	}
}

// slowInit may be called concurrently by multiple goroutines, so it requires
// synchronization with a "lock" mechanism.
//
//go:noinline
func (m *Map[K, V]) slowInit() *mapTable {
	rs := m.beginRebuild(mapRebuildBlockWritersHint)
	if rs == nil {
		// Another goroutine is initializing, wait for it to complete
		rs = (*rebuildState)(loadPtr(&m.rs))
		if rs != nil {
			rs.latch.Wait()
		}
		// Now the table should be initialized
		return (*mapTable)(loadPtr(&m.table))
	}

	// Although the table is always changed when rs is not nil,
	// it might have been changed before that.
	table := (*mapTable)(loadPtr(&m.table))
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

func (m *Map[K, V]) beginRebuild(hint mapRebuildHint) *rebuildState {
	if loadPtr(&m.rs) != nil {
		return nil
	}
	rs := &rebuildState{hint: hint}
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return nil
	}
	return rs
}

func (m *Map[K, V]) endRebuild(rs *rebuildState) {
	atomic.StorePointer(&m.rs, nil)
	rs.latch.Open()
}

//go:noinline
func (m *Map[K, V]) tryResize(hint mapRebuildHint, newLen uintptr) bool {
	rs := m.beginRebuild(hint)
	if rs == nil {
		return false
	}

	table := (*mapTable)(loadPtr(&m.table))
	tableLen := table.mask + 1
	if hint == mapGrowHint {
		if newLen <= tableLen {
			m.endRebuild(rs)
			return true
		}
		atomic.AddUint32(&m.growths, 1)
	} else {
		if newLen >= tableLen || newLen < m.minLen {
			m.endRebuild(rs)
			return true
		}
		atomic.AddUint32(&m.shrinks, 1)
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
	newTable := newMapTable(newLen, cpus)
	atomic.StorePointer(&rs.newTable, unsafe.Pointer(newTable))
	m.helpCopyAndWait(rs)
	return true
}

//go:noinline
func (m *Map[K, V]) helpCopyAndWait(rs *rebuildState) {
	newTable := (*mapTable)(loadPtr(&rs.newTable))
	newLen := newTable.mask + 1
	oldTable := (*mapTable)(rs.oldTable)
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
			// Copying completed
			atomic.StorePointer(&m.table, unsafe.Pointer(newTable))
			m.endRebuild(rs)
			return
		}
	}
}

func (m *Map[K, V]) copyBucket(
	table *mapTable,
	start, end uintptr,
	oldLen, baseLen uintptr,
	newTable *mapTable,
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
			for {
				meta := loadUint64Fast(&b.meta)
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := (*entry_[K, V])(*b.At(j))
					var hash uintptr
					var h1v uintptr
					var h2v uint8
					if opt.EmbeddedHash_ {
						hash = e.GetHash()
						if m.intKey {
							h1v = hash / entriesPerBucket
							h2v = h2(hash ^ (hash >> 16))
						} else {
							h1v = h1(hash)
							h2v = h2(hash)
						}
					} else {
						if m.intKey {
							hash = intHash[K](noescape(unsafe.Pointer(&e.key)))
							h1v = hash / entriesPerBucket
							h2v = h2(hash ^ (hash >> 16))
						} else {
							hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
							h1v = h1(hash)
							h2v = h2(hash)
						}
					}
					idx := mask & h1v
					destB := newTable.buckets.At(idx)
					// Append entry to the destination bucket
					for {
						meta := destB.meta
						if empty := (^meta) & metaMask; empty != 0 {
							emptyIdx := firstMarkedByteIndex(empty)
							destB.meta = setByte(meta, h2v, emptyIdx)
							*destB.At(emptyIdx) = unsafe.Pointer(e)
							break
						}
						if meta&opNextMask == 0 {
							destB.next = unsafe.Pointer(&bucket{
								meta:    setByte(metaEmpty, h2v, 0),
								entries: [entriesPerBucket]unsafe.Pointer{unsafe.Pointer(e)},
							})
							destB.meta = meta | opNextMask
							break
						}
						destB = (*bucket)(destB.next)
					}
					copied++
				}
				if meta&opNextMask == 0 {
					break
				}
				b = (*bucket)(b.next)
			}
			srcB.Unlock()
		}
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

// AddSize atomically adds delta to the size counter for the given bucket index.
//
//go:nosplit
func (t *mapTable) AddSize(idx, delta uintptr) uintptr {
	return atomic.AddUintptr(&t.size.At(t.sizeMask&idx).c, delta)
}

// SumSize calculates the total number of entries in the table
// by summing all counter-stripes.
//
//go:nosplit
func (t *mapTable) SumSize() uintptr {
	var sum uintptr
	for i := uintptr(0); i <= t.sizeMask; i++ {
		sum += loadUintptr(&t.size.At(i).c)
	}
	return sum
}

//go:nosplit
func (b *bucket) At(i uintptr) *unsafe.Pointer {
	return (*unsafe.Pointer)(
		unsafe.Add(
			unsafe.Pointer(&b.entries),
			i*unsafe.Sizeof(unsafe.Pointer(nil)),
		),
	)
}

// Lock acquires a spinlock for the bucket using embedded metadata.
// Uses atomic operations on the meta field to avoid false sharing overhead.
// Implements optimistic locking with fallback to spinning.
func (b *bucket) Lock() {
	// Inline BitLockUint64(&b.meta, opLockMask)
	cur := atomic.LoadUint64(&b.meta)
	if !atomic.CompareAndSwapUint64(&b.meta, cur&^opLockMask, cur|opLockMask) {
		slowBitLockUint64(&b.meta, opLockMask)
	}
}

//go:nosplit
func (b *bucket) Unlock() {
	BitUnlockUint64(&b.meta, opLockMask)
}

//go:nosplit
func (b *bucket) UnlockWithMeta(meta uint64) {
	BitUnlockWithStoreUint64(&b.meta, opLockMask, meta)
}
