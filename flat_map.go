//go:build !race

package cc

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// FlatMap implements a flat hash map using seqlock.
// Table and key/value pairs are stored inline (flat).
// Value size is not limited by the CPU word size.
// Readers use per-bucket seqlock: even sequence means stable; writers
// flip the bucket sequence to odd during mutation, then even again.
//
// Concurrency model:
//   - Readers: read s1=seq (must be even), then meta/entries, then s2=seq;
//     if s1!=s2 or s1 is odd, retry the bucket.
//   - Writers: take the root-bucket lock (opLock in meta), then on the target
//     bucket: seq++, apply changes, seq++, finally release the root lock.
//   - Resize: copy under the root-bucket lock using the same discipline.
//
// Notes:
//   - Reuses Map constants and compile-time bucket sizing (entriesPerBucket).
//   - Buckets are packed without padding for cache-friendly layout.
type FlatMap[K comparable, V any] struct {
	_        noCopy
	table    SeqLockSlot[flatTable[K, V]]
	tableSeq SeqLock32 // seqlock of table
	intKey   bool
	shrinkOn bool // [WithAutoShrink]
	seed     uintptr
	keyHash  HashFunc
	valEqual EqualFunc
	rs       unsafe.Pointer // [*flatRebuildState]
	minLen   uintptr        // [WithCapacity]
}

type flatRebuildState[K comparable, V any] struct {
	hint        mapRebuildHint
	newTableSeq SeqLock32 // seqlock of new table
	newTable    SeqLockSlot[flatTable[K, V]]
	latch       Latch
	oldTable    flatTable[K, V]
	chunks      uint32  // number of chunks for resizing
	chunkSz     uintptr // size of each chunk for resizing
	process     uint32  // atomic
	completed   uint32  // atomic
}

type flatTable[K comparable, V any] struct {
	buckets  unsafe.Pointer
	mask     uintptr
	size     unsafeSlice[counterStripe]
	sizeMask uintptr
}

type flatBucketHeader struct {
	_    [0]atomic.Uint64
	meta uint64         // op byte + h2 bytes
	seq  SeqLock        // seqlock of bucket
	next unsafe.Pointer // [*flatBucketHeader]
}

type flatBucketNoHash[K comparable, V any] struct {
	flatBucketHeader
	entries [entriesPerBucket]SeqLockSlot[entryNoHash[K, V]]
}

type flatBucketWithHash[K comparable, V any] struct {
	flatBucketHeader
	entries [entriesPerBucket]SeqLockSlot[entryWithHash[K, V]]
}

//go:nosplit
func (m *FlatMap[K, V]) bucketAt(buckets unsafe.Pointer, i uintptr) *flatBucketHeader {
	if cacheHash[K]() {
		return (*flatBucketHeader)(unsafe.Add(buckets, i*unsafe.Sizeof(flatBucketWithHash[K, V]{})))
	}
	return (*flatBucketHeader)(unsafe.Add(buckets, i*unsafe.Sizeof(flatBucketNoHash[K, V]{})))
}

//go:nosplit
func (m *FlatMap[K, V]) entryAt(b *flatBucketHeader, j uintptr) unsafe.Pointer {
	headerSize := unsafe.Sizeof(flatBucketHeader{})
	if cacheHash[K]() {
		slotSize := unsafe.Sizeof(SeqLockSlot[entryWithHash[K, V]]{})
		return unsafe.Add(unsafe.Pointer(b), headerSize+j*slotSize)
	}
	slotSize := unsafe.Sizeof(SeqLockSlot[entryNoHash[K, V]]{})
	return unsafe.Add(unsafe.Pointer(b), headerSize+j*slotSize)
}

//go:nosplit
func (m *FlatMap[K, V]) newFlatBucket(meta uint64, key K, value V, hash uintptr) unsafe.Pointer {
	if cacheHash[K]() {
		b := &flatBucketWithHash[K, V]{
			flatBucketHeader: flatBucketHeader{
				meta: meta,
			},
		}
		b.entries[0].buf.hash = hash
		b.entries[0].buf.key = key
		b.entries[0].buf.value = value
		return unsafe.Pointer(b)
	}
	b := &flatBucketNoHash[K, V]{
		flatBucketHeader: flatBucketHeader{
			meta: meta,
		},
	}
	b.entries[0].buf.key = key
	b.entries[0].buf.value = value
	return unsafe.Pointer(b)
}

// NewFlatMap creates a new seqlock-based flat hash map.
//
// Highlights:
//   - Optimistic reads via per-bucket seqlock; brief spinning under
//     contention.
//   - Writes coordinate via a lightweight root-bucket lock and per-bucket
//     seqlock fencing.
//   - Parallel resize (grow/shrink) with cooperative copying by readers and
//     writers.
//
// Configuration options (aligned with Map):
//   - WithCapacity(cap): pre-allocate capacity to reduce early resizes.
//   - WithAutoShrink(): enable automatic shrinking when load drops.
//   - WithKeyHasher / WithKeyHasherUnsafe / WithBuiltInHasher: custom or
//     built-in hashing.
//
// Example:
//
//	m := NewFlatMap[string,int](WithCapacity(1024), WithAutoShrink())
//	m.Store("a", 1)
//	v, ok := m.Load("a")
func NewFlatMap[K comparable, V any](
	options ...func(*MapConfig),
) *FlatMap[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m := &FlatMap[K, V]{}
	m.init(noEscape(&cfg))
	return m
}

func (m *FlatMap[K, V]) init(
	cfg *MapConfig,
) {
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
	m.shrinkOn = cfg.autoShrink
	newLen := calcTableLen(cfg.capacity)
	m.minLen = newLen
	newTable := newFlatTable[K, V](newLen, maxProcs())
	SeqLockWriteLocked32(&m.tableSeq, &m.table, newTable)
}

// Load retrieves the value for a key.
//
//   - Per-bucket seqlock read; an even and stable sequence yields
//     a consistent snapshot.
//   - Short spinning on observed writes (odd seq) or
//     instability.
//   - Provides stable latency under high concurrency.
func (m *FlatMap[K, V]) Load(key K) (value V, ok bool) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
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
	b := m.bucketAt(table.buckets, idx)
retry:
	for {
		s1, ok := b.seq.BeginRead()
		if !ok {
			continue retry
		}
		meta := loadUint64Fast(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			if cacheHash[K]() {
				var eKey K
				var eVal V
				var eHash uintptr
				slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(b, j))
				e := slot.ReadUnfenced()
				eHash, eKey, eVal = e.hash, e.key, e.value
				if !b.seq.EndRead(s1) {
					continue retry
				}
				if eHash == hash && eKey == key {
					return eVal, true
				}
			} else {
				var eKey K
				var eVal V
				slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(b, j))
				e := slot.ReadUnfenced()
				eKey, eVal = e.key, e.value
				if !b.seq.EndRead(s1) {
					continue retry
				}
				if eKey == key {
					return eVal, true
				}
			}
		}
		if meta&opNextMask == 0 {
			return *new(V), false
		}
		b = (*flatBucketHeader)(loadPtr(&b.next))
	}
}

// Store sets the value for a key.
func (m *FlatMap[K, V]) Store(key K, value V) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		m.slowInit()
		table = SeqLockRead32(&m.tableSeq, &m.table)
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
	root := m.bucketAt(table.buckets, idx)
	// Fast path: lock-free read

	for b := root; ; b = (*flatBucketHeader)(loadPtr(&b.next)) {
		s1, ok := b.seq.BeginRead()
		if !ok {
			goto slowPath
		}
		meta := loadUint64Fast(&b.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			var eKey K
			var eVal V
			var eHash uintptr
			if cacheHash[K]() {
				slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(b, j))
				e := slot.ReadUnfenced()
				eHash, eKey, eVal = e.hash, e.key, e.value
			} else {
				slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(b, j))
				e := slot.ReadUnfenced()
				eKey, eVal = e.key, e.value
			}
			if !b.seq.EndRead(s1) {
				goto slowPath
			}
			if (!cacheHash[K]() || eHash == hash) && eKey == key {
				// valEqual: skip write if value unchanged
				if m.valEqual != nil && m.valEqual(
					noescape(unsafe.Pointer(&eVal)),
					noescape(unsafe.Pointer(&value)),
				) {
					return
				}
				goto slowPath
			}
		}
		if meta&opNextMask == 0 {
			break
		}
	}

slowPath:
	root.Lock()
	// Help finishing rebuild if needed
	if rs := (*flatRebuildState[K, V])(loadPtr(&m.rs)); rs != nil {
		switch rs.hint {
		case mapGrowHint, mapShrinkHint:
			if rs.newTableSeq.Ready() {
				root.Unlock()
				m.helpCopyAndWait(rs)
				table = SeqLockRead32(&m.tableSeq, &m.table)
				idx = table.mask & h1v
				root = m.bucketAt(table.buckets, idx)
				goto slowPath
			}
		case mapRebuildBlockWritersHint:
			root.Unlock()
			rs.latch.Wait()
			table = SeqLockRead32(&m.tableSeq, &m.table)
			idx = table.mask & h1v
			root = m.bucketAt(table.buckets, idx)
			goto slowPath
		default:
			// mapRebuildAllowWritersHint: allow concurrent writers
		}
	}

	if newTable := SeqLockRead32(&m.tableSeq, &m.table); newTable.buckets != table.buckets {
		root.Unlock()
		table = newTable
		idx = table.mask & h1v
		root = m.bucketAt(table.buckets, idx)
		goto slowPath
	}

	var (
		emptyM uint64
		emptyB *flatBucketHeader
		emptyI uintptr
	)
	lastB := root
	for {
		meta := loadUint64Fast(&lastB.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j := firstMarkedByteIndex(marked)
			var eKey K
			if cacheHash[K]() {
				slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(lastB, j))
				if slot.Ptr().hash != hash {
					continue
				}
				eKey = slot.Ptr().key
			} else {
				slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(lastB, j))
				eKey = slot.Ptr().key
			}
			if eKey == key {
				// Update
				lastB.seq.BeginWriteLocked()
				if cacheHash[K]() {
					slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(lastB, j))
					slot.WriteUnfenced(entryWithHash[K, V]{hash: hash, key: key, value: value})
				} else {
					slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(lastB, j))
					slot.WriteUnfenced(entryNoHash[K, V]{key: key, value: value})
				}
				lastB.seq.EndWriteLocked()
				root.Unlock()
				return
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
		lastB = (*flatBucketHeader)(lastB.next)
	}

	if emptyB == nil {
		// append new bucket
		newB := m.newFlatBucket(setByte(emptyM, h2v, 0), key, value, hash)
		storePtr(&lastB.next, newB)
		newMeta := loadUint64Fast(&lastB.meta) | opNextMask
		if lastB == root {
			root.UnlockWithMeta(newMeta)
		} else {
			storeUint64(&lastB.meta, newMeta)
			root.Unlock()
		}

		localSize := int(table.AddSize(idx, 1))
		// Check if the table needs to grow
		const capFactor = float64(entriesPerBucket) * loadFactor
		tableLen := table.mask + 1
		growCap := int(float64(tableLen) * capFactor)
		stripeCap := int(growCap >> bits.TrailingZeros32(uint32(table.sizeMask+1)))
		if localSize >= stripeCap {
			if loadPtr(&m.rs) == nil {
				if table.SumSize() >= growCap {
					m.tryResize(mapGrowHint, tableLen<<1)
				}
			}
		}
		return
	}
	// Insert into empty slot
	if cacheHash[K]() {
		slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(emptyB, emptyI))
		slot.WriteUnfenced(entryWithHash[K, V]{hash: hash, key: key, value: value})
	} else {
		slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(emptyB, emptyI))
		slot.WriteUnfenced(entryNoHash[K, V]{key: key, value: value})
	}
	newMeta := setByte(emptyM, h2v, emptyI)
	if emptyB == root {
		root.UnlockWithMeta(newMeta)
	} else {
		storeUint64(&emptyB.meta, newMeta)
		root.Unlock()
	}
	table.AddSize(idx, 1)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMap[K, V]) LoadOrStore(
	key K,
	value V,
) (actual V, loaded bool) {
	return m.compute(&key, unsafe.Pointer(&value), computeInit|computeSkipIfFound|computeUsesValue)
}

// LoadOrStoreFn loads the value for a key if present.
// Otherwise, it stores and returns the value returned by valueFn.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMap[K, V]) LoadOrStoreFn(
	key K,
	valueFn func() V,
) (actual V, loaded bool) {
	fn := func(e *MapEntry[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(valueFn())
	}
	return m.compute(&key, unsafe.Pointer(&fn), computeInit|computeSkipIfFound)
}

// LoadAndUpdate updates the value for key if it exists, returning the previous
// value. The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	return m.compute(&key, unsafe.Pointer(&value), computeSkipIfNotFound|computeUsesValue)
}

// LoadAndDelete deletes the value for a key, returning the previous value.
// The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.compute(&key, nil, computeSkipIfNotFound|computeUsesValue)
}

// Swap stores value for key and returns the previous value if any.
// The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	return m.compute(&key, unsafe.Pointer(&value), computeInit|computeUsesValue)
}

// Delete deletes the value for a key.
func (m *FlatMap[K, V]) Delete(key K) {
	m.compute(&key, nil, computeSkipIfNotFound|computeUsesValue)
}

// CompareAndSwap atomically replaces an existing value with a new value.
// If the existing value matches the expected value.
func (m *FlatMap[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndSwap when value is not of comparable type")
	}
	fn := func(e *MapEntry[K, V]) {
		if e.Loaded() {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.entry.value)),
				noescape(unsafe.Pointer(&old)),
			) {
				e.Update(new)
				swapped = true
			}
		}
	}
	m.compute(&key, unsafe.Pointer(&fn), computeSkipIfNotFound)
	return swapped
}

// CompareAndDelete atomically deletes an existing entry.
// If its value matches the expected value.
func (m *FlatMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		return false
	}
	if m.valEqual == nil {
		panic("called CompareAndDelete when value is not of comparable type")
	}
	fn := func(e *MapEntry[K, V]) {
		if e.Loaded() {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.entry.value)),
				noescape(unsafe.Pointer(&old)),
			) {
				e.Delete()
				deleted = true
			}
		}
	}
	m.compute(&key, unsafe.Pointer(&fn), computeSkipIfNotFound)
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
//   - actual: the current value in the map after the operation
//   - loaded: true if the key existed before the operation
func (m *FlatMap[K, V]) Compute(
	key K,
	fn func(e *MapEntry[K, V]),
) (actual V, loaded bool) {
	return m.compute(&key, unsafe.Pointer(&fn), computeInit)
}

func (m *FlatMap[K, V]) compute(
	key *K,
	val unsafe.Pointer, // *V or func(e *MapEntry[K, V])
	flags uint8,
) (actual V, loaded bool) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		if flags&computeInit == 0 {
			return *new(V), false
		}
		m.slowInit()
		table = SeqLockRead32(&m.tableSeq, &m.table)
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
	root := m.bucketAt(table.buckets, idx)
	// Fast path: lock-free read
	if flags&(computeSkipIfFound|computeSkipIfNotFound) != 0 {
		b := root
		for {
			s1, ok := b.seq.BeginRead()
			if !ok {
				goto slowPath
			}
			meta := loadUint64Fast(&b.meta)
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				var eKey K
				var eVal V
				var eHash uintptr
				if cacheHash[K]() {
					slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(b, j))
					e := slot.ReadUnfenced()
					eHash, eKey, eVal = e.hash, e.key, e.value
				} else {
					slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(b, j))
					e := slot.ReadUnfenced()
					eKey, eVal = e.key, e.value
				}
				if !b.seq.EndRead(s1) {
					goto slowPath
				}
				if (!cacheHash[K]() || eHash == hash) && eKey == *key {
					if flags&computeSkipIfFound != 0 {
						return eVal, true
					}
					if flags&computeUsesValue != 0 {
						if val != nil {
							if m.valEqual != nil && m.valEqual(
								noescape(unsafe.Pointer(&eVal)),
								noescape(val),
							) {
								return eVal, true
							}
						}
					}
					goto slowPath
				}
			}
			if meta&opNextMask == 0 {
				break
			}
			b = (*flatBucketHeader)(loadPtr(&b.next))
		}
		// Key not found in fast path
		if flags&computeSkipIfNotFound != 0 {
			return *new(V), false
		}
	}

slowPath:
	root.Lock()

	// Help finishing rebuild if needed
	if flags&computeIgnoreHint == 0 {
		if rs := (*flatRebuildState[K, V])(loadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if rs.newTableSeq.Ready() {
					root.Unlock()
					m.helpCopyAndWait(rs)
					table = SeqLockRead32(&m.tableSeq, &m.table)
					idx = table.mask & h1v
					root = m.bucketAt(table.buckets, idx)
					goto slowPath
				}
			case mapRebuildBlockWritersHint:
				root.Unlock()
				rs.latch.Wait()
				table = SeqLockRead32(&m.tableSeq, &m.table)
				idx = table.mask & h1v
				root = m.bucketAt(table.buckets, idx)
				goto slowPath
			default:
				// mapRebuildAllowWritersHint: allow concurrent writers
			}
		}
	}
	if newTable := SeqLockRead32(&m.tableSeq, &m.table); newTable.buckets != table.buckets {
		root.Unlock()
		table = newTable
		idx = table.mask & h1v
		root = m.bucketAt(table.buckets, idx)
		goto slowPath
	}

	var (
		meta   uint64
		j      uintptr
		emptyM uint64
		emptyB *flatBucketHeader
		emptyI uintptr
	)
	it := MapEntry[K, V]{entry: entry_[K, V]{key: *key}}
	if cacheHash[K]() {
		it.entry.hash = hash
	}

	lastB := root
findLoop:
	for {
		meta = loadUint64Fast(&lastB.meta)
		for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
			j = firstMarkedByteIndex(marked)
			var eKey K
			var eVal V
			if cacheHash[K]() {
				slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(lastB, j))
				e := slot.Ptr()
				if e.hash != hash {
					continue
				}
				eKey, eVal = e.key, e.value
			} else {
				slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(lastB, j))
				e := slot.Ptr()
				eKey, eVal = e.key, e.value
			}

			if eKey == *key {
				it.entry.value, it.loaded = eVal, true
				break findLoop
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
		lastB = (*flatBucketHeader)(lastB.next)
	}

	// --- Compute Logic ---
	retV := it.entry.value
	if flags == computeInit|computeSkipIfFound|computeUsesValue { //nolint:staticcheck
		// LoadOrStore
		if !it.loaded {
			it.entry.value = *(*V)(val)
			it.op = updateOp
			retV = it.entry.value
		}
	} else if flags == computeSkipIfNotFound|computeUsesValue {
		if it.loaded {
			if val != nil {
				// LoadAndUpdate
				it.entry.value = *(*V)(val)
				it.op = updateOp
			} else {
				// LoadAndDelete, Delete
				it.op = deleteOp
			}
		}
	} else if flags == computeInit|computeUsesValue {
		// Swap, Store
		it.entry.value = *(*V)(val)
		it.op = updateOp
	} else {
		// Compute, LoadOrStoreFn, CompareAnd...
		(*(*func(e *MapEntry[K, V]))(val))(noEscape(&it))
		retV = it.entry.value
	}

	switch it.op {
	case updateOp:
		if it.loaded {
			// Update
			lastB.seq.BeginWriteLocked()
			if cacheHash[K]() {
				slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(lastB, j))
				slot.WriteUnfenced(entryWithHash[K, V]{hash: hash, key: *key, value: it.entry.value})
			} else {
				slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(lastB, j))
				slot.WriteUnfenced(entryNoHash[K, V]{key: *key, value: it.entry.value})
			}
			lastB.seq.EndWriteLocked()
			root.Unlock()
			return retV, it.loaded
		}
		if emptyB == nil {
			// append new bucket
			newB := m.newFlatBucket(setByte(emptyM, h2v, 0), *key, it.entry.value, hash)
			storePtr(&lastB.next, newB)
			newMeta := loadUint64Fast(&lastB.meta) | opNextMask
			if lastB == root {
				root.UnlockWithMeta(newMeta)
			} else {
				storeUint64(&lastB.meta, newMeta)
				root.Unlock()
			}

			localSize := int(table.AddSize(idx, 1))
			// Check if the table needs to grow
			const capFactor = float64(entriesPerBucket) * loadFactor
			tableLen := table.mask + 1
			growCap := int(float64(tableLen) * capFactor)
			stripeCap := int(growCap >> bits.TrailingZeros32(uint32(table.sizeMask+1)))
			if localSize >= stripeCap {
				if loadPtr(&m.rs) == nil {
					if table.SumSize() >= growCap {
						m.tryResize(mapGrowHint, tableLen<<1)
					}
				}
			}
			return retV, it.loaded
		}
		// Insert into empty slot
		if cacheHash[K]() {
			slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(emptyB, emptyI))
			slot.WriteUnfenced(entryWithHash[K, V]{hash: hash, key: *key, value: it.entry.value})
		} else {
			slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(emptyB, emptyI))
			slot.WriteUnfenced(entryNoHash[K, V]{key: *key, value: it.entry.value})
		}
		newMeta := setByte(emptyM, h2v, emptyI)
		if emptyB == root {
			root.UnlockWithMeta(newMeta)
		} else {
			storeUint64(&emptyB.meta, newMeta)
			root.Unlock()
		}
		table.AddSize(idx, 1)
		return retV, it.loaded
	case deleteOp:
		if !it.loaded {
			root.Unlock()
			return retV, it.loaded
		}
		newMeta := setByte(meta, h2Empty, j)
		storeUint64(&lastB.meta, newMeta)
		lastB.seq.BeginWriteLocked()
		if cacheHash[K]() {
			slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(lastB, j))
			slot.WriteUnfenced(entryWithHash[K, V]{})
		} else {
			slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(lastB, j))
			slot.WriteUnfenced(entryNoHash[K, V]{})
		}
		lastB.seq.EndWriteLocked()

		root.Unlock()
		table.AddSize(idx, ^uintptr(0))
		// Check if table shrinking is needed
		if m.shrinkOn {
			if newMeta&metaDataMask == metaEmpty {
				if loadPtr(&m.rs) == nil {
					tableLen := table.mask + 1
					if m.minLen < tableLen {
						size := table.SumSize()
						if size < int(tableLen*entriesPerBucket/shrinkFraction) {
							m.tryResize(mapShrinkHint, tableLen>>1)
						}
					}
				}
			}
		}
		return retV, it.loaded
	default:
		// cancelOp: No-op
		root.Unlock()
		return retV, it.loaded
	}
}

// Range iterates all entries using per-bucket seqlock reads.
//
//   - Copies a consistent snapshot from each bucket when the sequence is
//     stable; otherwise briefly spins and retries.
//   - Yields outside of locks to minimize contention.
//   - Returning false from the callback stops iteration early.
func (m *FlatMap[K, V]) Range(yield func(K, V) bool) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		return
	}

	var meta uint64
	for i := uintptr(0); i <= table.mask; i++ {
		b := m.bucketAt(table.buckets, i)
	retry:
		for {
			s1, ok := b.seq.BeginRead()
			if !ok {
				continue retry
			}
			meta = loadUint64Fast(&b.meta)

			if cacheHash[K]() {
				var cache [entriesPerBucket]entryWithHash[K, V]
				var cacheCount uintptr
				unsafeCache := toUnsafeSlice(&cache[0])
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(b, j))
					*unsafeCache.At(cacheCount) = slot.ReadUnfenced()
					cacheCount++
				}
				if !b.seq.EndRead(s1) {
					continue retry
				}
				for j := range cacheCount {
					kv := unsafeCache.At(j)
					if !yield(kv.key, kv.value) {
						return
					}
				}
			} else {
				var cache [entriesPerBucket]entryNoHash[K, V]
				var cacheCount uintptr
				unsafeCache := toUnsafeSlice(&cache[0])
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(b, j))
					*unsafeCache.At(cacheCount) = slot.ReadUnfenced()
					cacheCount++
				}
				if !b.seq.EndRead(s1) {
					continue retry
				}
				for j := range cacheCount {
					kv := unsafeCache.At(j)
					if !yield(kv.key, kv.value) {
						return
					}
				}
			}
			if meta&opNextMask == 0 {
				break
			}
			b = (*flatBucketHeader)(loadPtr(&b.next))
		}
	}
}

// All returns an iterator function for use with range-over-func.
// It provides the same functionality as Range but in iterator form.
//
//go:nosplit
func (m *FlatMap[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the number of key-value pairs in the map.
// This operation sums counters across all size stripes for an approximate
// count.
func (m *FlatMap[K, V]) Size() int {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		return 0
	}

	return max(table.SumSize(), 0)
}

// ToMap collect up to limit entries into a map[K]V, limit < 0 is no limit.
func (m *FlatMap[K, V]) ToMap(limit ...int) map[K]V {
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
func (m *FlatMap[K, V]) Entries() func(yield func(e *MapEntry[K, V]) bool) {
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
func (m *FlatMap[K, V]) ComputeRange(yield func(e *MapEntry[K, V]) bool) {
	m.computeRange(yield, false)
}

func (m *FlatMap[K, V]) computeRange(yield func(e *MapEntry[K, V]) bool, ignoreRebuildState bool) {
restart:
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		return
	}
	it := MapEntry[K, V]{
		loaded: true,
	}

	for i := uintptr(0); i <= table.mask; i++ {
		root := m.bucketAt(table.buckets, i)
		root.Lock()

		if !ignoreRebuildState {
			if rs := (*flatRebuildState[K, V])(loadPtr(&m.rs)); rs != nil {
				switch rs.hint {
				case mapGrowHint, mapShrinkHint:
					if rs.newTableSeq.Ready() {
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

			if newTable := SeqLockRead32(&m.tableSeq, &m.table); newTable.buckets != table.buckets {
				root.Unlock()
				goto restart
			}
		}

		b := root
		for {
			meta := loadUint64Fast(&b.meta)
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)

				if cacheHash[K]() {
					slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(b, j))
					e := slot.Ptr()
					it.entry.hash = e.hash
					it.entry.key, it.entry.value = e.key, e.value
				} else {
					slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(b, j))
					e := slot.Ptr()
					it.entry.key, it.entry.value = e.key, e.value
				}
				it.op = cancelOp
				shouldContinue := yield(noEscape(&it))
				switch it.op {
				case updateOp:
					b.seq.BeginWriteLocked()
					if cacheHash[K]() {
						slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(b, j))
						slot.WriteUnfenced(entryWithHash[K, V]{hash: it.entry.hash, key: it.entry.key, value: it.entry.value})
					} else {
						slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(b, j))
						slot.WriteUnfenced(entryNoHash[K, V]{key: it.entry.key, value: it.entry.value})
					}
					b.seq.EndWriteLocked()
				case deleteOp:
					meta = setByte(meta, h2Empty, j)
					storeUint64(&b.meta, meta)
					b.seq.BeginWriteLocked()
					if cacheHash[K]() {
						slot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(b, j))
						slot.WriteUnfenced(entryWithHash[K, V]{})
					} else {
						slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(b, j))
						slot.WriteUnfenced(entryNoHash[K, V]{})
					}
					b.seq.EndWriteLocked()
					table.AddSize(i, ^uintptr(0))
				default:
					// cancelOp: No-op
				}
				if !shouldContinue {
					root.Unlock()
					return
				}
			}
			if meta&opNextMask == 0 {
				break
			}
			b = (*flatBucketHeader)(b.next)
		}
		root.Unlock()
	}
}

// Clear clears all key-value pairs from the map.
func (m *FlatMap[K, V]) Clear() {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		return
	}

	m.rebuild(mapRebuildBlockWritersHint, func(_ *MapRebuild[K, V]) {
		newTable := newFlatTable[K, V](m.minLen, maxProcs())
		SeqLockWriteLocked32(&m.tableSeq, &m.table, newTable)
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
func (m *FlatMap[K, V]) Grow(sizeAdd int) {
	if sizeAdd <= 0 {
		return
	}
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		m.slowInit()
	}
	m.doResize(mapGrowHint, sizeAdd)
}

// Shrink reduces the capacity to fit the current size,
// always executes regardless of WithAutoShrink.
func (m *FlatMap[K, V]) Shrink() {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		return
	}
	m.doResize(mapShrinkHint, 0)
}

func (m *FlatMap[K, V]) doResize(hint mapRebuildHint, sizeAdd int) {
	for {
		// Resize check
		table := SeqLockRead32(&m.tableSeq, &m.table)
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
		if rs := (*flatRebuildState[K, V])(loadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if rs.newTableSeq.Ready() {
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
func (m *FlatMap[K, V]) CloneTo(clone *FlatMap[K, V]) {
	clone.Clear()
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets == nil {
		return
	}
	clone.intKey = m.intKey
	clone.shrinkOn = m.shrinkOn
	clone.seed = m.seed
	clone.keyHash = m.keyHash
	clone.valEqual = m.valEqual
	clone.minLen = m.minLen
	newLen := calcTableLen(table.SumSize())
	newTable := newFlatTable[K, V](newLen, maxProcs())
	SeqLockWriteLocked32(&clone.tableSeq, &clone.table, newTable)
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
func (m *FlatMap[K, V]) Rebuild(fn func(m *MapRebuild[K, V])) {
	m.rebuild(mapRebuildBlockWritersHint, fn)
}

// rebuild reorganizes the map. Only these hints are supported:
//   - mapRebuildAllowWritersHint: allows concurrent reads/writes
//   - mapRebuildBlockWritersHint: allows concurrent reads
func (m *FlatMap[K, V]) rebuild(
	rebuildHint mapRebuildHint,
	fn func(m *MapRebuild[K, V]),
) {
	for {
		// Help finishing rebuild if needed
		if rs := (*flatRebuildState[K, V])(loadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if rs.newTableSeq.Ready() {
					m.helpCopyAndWait(rs)
				} else {
					runtime.Gosched()
					continue
				}
			default:
				rs.latch.Wait()
			}
		}
		if rs := m.beginRebuild(rebuildHint); rs != nil {
			fn(noEscape(&MapRebuild[K, V]{f: m}))
			m.endRebuild(rs)
			return
		}
	}
}

//go:noinline
func (m *FlatMap[K, V]) slowInit() {
	rs := m.beginRebuild(mapRebuildBlockWritersHint)
	if rs == nil {
		rs = (*flatRebuildState[K, V])(loadPtr(&m.rs))
		if rs != nil {
			rs.latch.Wait()
		}
		return
	}
	// The table may have been altered prior to our changes.
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets != nil {
		m.endRebuild(rs)
		return
	}
	var cfg MapConfig
	m.init(&cfg)
	m.endRebuild(rs)
}

func (m *FlatMap[K, V]) beginRebuild(hint mapRebuildHint) *flatRebuildState[K, V] {
	if loadPtr(&m.rs) != nil {
		return nil
	}
	rs := &flatRebuildState[K, V]{
		hint: hint,
	}
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return nil
	}
	return rs
}

func (m *FlatMap[K, V]) endRebuild(rs *flatRebuildState[K, V]) {
	atomic.StorePointer(&m.rs, nil)
	rs.latch.Open()
}

//go:noinline
func (m *FlatMap[K, V]) tryResize(hint mapRebuildHint, newLen uintptr) bool {
	rs := m.beginRebuild(hint)
	if rs == nil {
		return false
	}

	table := m.table.Ptr()
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
	rs.oldTable = *table
	newTable := newFlatTable[K, V](newLen, cpus)
	SeqLockWriteLocked32(&rs.newTableSeq, &rs.newTable, newTable)
	m.helpCopyAndWait(rs)
	return true
}

//go:noinline
func (m *FlatMap[K, V]) helpCopyAndWait(rs *flatRebuildState[K, V]) {
	newTable := SeqLockRead32(&rs.newTableSeq, &rs.newTable)
	newLen := newTable.mask + 1
	oldTable := rs.oldTable
	oldLen := oldTable.mask + 1
	chunks := rs.chunks
	chunkSz := rs.chunkSz
	baseLen := min(newLen, oldLen)
	for {
		process := atomic.AddUint32(&rs.process, 1)
		if process > chunks {
			rs.latch.Wait()
			return
		}
		process--
		start := uintptr(process) * chunkSz
		end := min(start+chunkSz, baseLen)
		m.copyBucket(&oldTable, start, end, oldLen, baseLen, &newTable)
		if atomic.AddUint32(&rs.completed, 1) == chunks {
			SeqLockWriteLocked32(&m.tableSeq, &m.table, newTable)
			m.endRebuild(rs)
			return
		}
	}
}

func (m *FlatMap[K, V]) copyBucket(
	table *flatTable[K, V],
	start, end uintptr,
	oldLen, baseLen uintptr,
	newTable *flatTable[K, V],
) {
	mask := newTable.mask
	var copied uintptr
	for i := start; i < end; i++ {
		// Visit all source buckets that map to this destination bucket.
		// In Grow, runs once. In Shrink, runs twice (usually).
		for srcIdx := i; srcIdx < oldLen; srcIdx += baseLen {
			srcB := m.bucketAt(table.buckets, srcIdx)
			srcB.Lock()
			b := srcB
			for {
				meta := loadUint64Fast(&b.meta)
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					if cacheHash[K]() {
						e := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(b, j)).Ptr()
						hash := e.hash
						var h1v uintptr
						var h2v uint8
						if m.intKey {
							h1v = hash / entriesPerBucket
							h2v = h2(hash ^ (hash >> 16))
						} else {
							h1v = h1(hash)
							h2v = h2(hash)
						}
						idx := mask & h1v
						destB := m.bucketAt(newTable.buckets, idx)
						for {
							destMeta := destB.meta
							if empty := (^destMeta) & metaMask; empty != 0 {
								emptyIdx := firstMarkedByteIndex(empty)
								destB.meta = setByte(destMeta, h2v, emptyIdx)
								destSlot := (*SeqLockSlot[entryWithHash[K, V]])(m.entryAt(destB, emptyIdx))
								destSlot.WriteUnfenced(entryWithHash[K, V]{hash: hash, key: e.key, value: e.value})
								break
							}
							if destMeta&opNextMask == 0 {
								newB := m.newFlatBucket(setByte(metaEmpty, h2v, 0), e.key, e.value, hash)
								storePtr(&destB.next, newB)
								destB.meta = destMeta | opNextMask
								break
							}
							destB = (*flatBucketHeader)(loadPtr(&destB.next))
						}
					} else {
						var hash uintptr
						var h1v uintptr
						var h2v uint8
						slot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(b, j))
						e := slot.Ptr()
						if m.intKey {
							hash = intHash[K](noescape(unsafe.Pointer(&e.key)))
							h1v = hash / entriesPerBucket
							h2v = h2(hash ^ (hash >> 16))
						} else {
							hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
							h1v = h1(hash)
							h2v = h2(hash)
						}
						idx := mask & h1v
						destB := m.bucketAt(newTable.buckets, idx)
						for {
							destMeta := destB.meta
							if empty := (^destMeta) & metaMask; empty != 0 {
								emptyIdx := firstMarkedByteIndex(empty)
								destB.meta = setByte(destMeta, h2v, emptyIdx)
								destSlot := (*SeqLockSlot[entryNoHash[K, V]])(m.entryAt(destB, emptyIdx))
								destSlot.WriteUnfenced(entryNoHash[K, V]{key: e.key, value: e.value})
								break
							}
							if destMeta&opNextMask == 0 {
								newB := m.newFlatBucket(setByte(metaEmpty, h2v, 0), e.key, e.value, hash)
								storePtr(&destB.next, newB)
								destB.meta = destMeta | opNextMask
								break
							}
							destB = (*flatBucketHeader)(loadPtr(&destB.next))
						}
					}
					copied++
				}
				if meta&opNextMask == 0 {
					break
				}
				b = (*flatBucketHeader)(loadPtr(&b.next))
			}
			srcB.Unlock()
		}
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

func newFlatTable[K comparable, V any](
	tableLen, cpus uintptr,
) flatTable[K, V] {
	sizeLen := calcSizeLen(tableLen, cpus)
	var buckets unsafe.Pointer
	if cacheHash[K]() {
		buckets = unsafe.Pointer(unsafe.SliceData(make([]flatBucketWithHash[K, V], tableLen)))
	} else {
		buckets = unsafe.Pointer(unsafe.SliceData(make([]flatBucketNoHash[K, V], tableLen)))
	}
	return flatTable[K, V]{
		buckets:  buckets,
		mask:     tableLen - 1,
		size:     makeUnsafeSlice[counterStripe](sizeLen),
		sizeMask: sizeLen - 1,
	}
}

//go:nosplit
func (t *flatTable[K, V]) AddSize(idx, delta uintptr) uintptr {
	return atomic.AddUintptr(&t.size.At(t.sizeMask&idx).c, delta)
}

//go:nosplit
func (t *flatTable[K, V]) SumSize() int {
	var sum uintptr
	for i := uintptr(0); i <= t.sizeMask; i++ {
		sum += loadUintptr(&t.size.At(i).c)
	}
	return int(sum)
}

func (b *flatBucketHeader) Lock() {
	// Inline BitLockUint64(&b.meta, opLockMask)
	cur := atomic.LoadUint64(&b.meta)
	if !atomic.CompareAndSwapUint64(&b.meta, cur&^opLockMask, cur|opLockMask) {
		slowBitLockUint64(&b.meta, opLockMask)
	}
}

//go:nosplit
func (b *flatBucketHeader) Unlock() {
	BitUnlockUint64(&b.meta, opLockMask)
}

//go:nosplit
func (b *flatBucketHeader) UnlockWithMeta(meta uint64) {
	BitUnlockWithStoreUint64(&b.meta, opLockMask, meta)
}
