package cc

import (
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/llxisdsh/cc/internal/opt"
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
	seed     uintptr
	keyHash  HashFunc
	valEqual EqualFunc
	tableSeq SeqLock32 // seqlock of table
	shrinkOn bool      // WithAutoShrink
	intKey   bool
	rs       unsafe.Pointer // *flatRebuildState[K, V]
}

type flatRebuildState[K comparable, V any] struct {
	hint        mapRebuildHint
	chunks      atomic.Int32
	newTable    SeqLockSlot[flatTable[K, V]]
	newTableSeq SeqLock32 // seqlock of new table
	process     atomic.Int32
	completed   atomic.Int32
	latch       Latch
}

type flatTable[K comparable, V any] struct {
	buckets  unsafeSlice[flatBucket[K, V]]
	mask     int
	size     unsafeSlice[counterStripe]
	sizeMask int
}

type flatBucket[K comparable, V any] struct {
	_       [0]atomic.Uint64
	meta    uint64         // op byte + h2 bytes
	seq     SeqLock        // seqlock of bucket
	next    unsafe.Pointer // *flatBucket[K,V]
	entries [entriesPerBucket]SeqLockSlot[entry_[K, V]]
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
	tableLen := calcTableLen(cfg.capacity)
	SeqLockWriteLocked32(&m.tableSeq, &m.table,
		newFlatTable[K, V](tableLen, maxProcs()))
}

//go:noinline
func (m *FlatMap[K, V]) slowInit() {
	rs, ok := m.beginRebuild(mapRebuildBlockWritersHint)
	if !ok {
		rs = (*flatRebuildState[K, V])(loadPtr(&m.rs))
		if rs != nil {
			rs.latch.Wait()
		}
		return
	}
	// The table may have been altered prior to our changes.
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets.ptr != nil {
		m.endRebuild(rs)
		return
	}
	var cfg MapConfig
	m.init(&cfg)
	m.endRebuild(rs)
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
	if table.buckets.ptr == nil {
		return *new(V), false
	}

	var hash uintptr
	var h1v int

	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(&key)))
		h1v = h1IntKey(hash)
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h1v = h1(hash)
	}

	h2v := h2(hash)
	h2w := broadcast(h2v)
	idx := table.mask & h1v
	b := table.buckets.At(idx)
	for {
		var spins int
	retry:
		s1, ok := b.seq.BeginRead()
		if !ok {
			delay(&spins)
			goto retry
		}
		meta := loadUint64Fast(&b.meta)
		//goland:noinspection GoBoolExpressions
		if !opt.Race_ {
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).ReadUnfenced()
				if !b.seq.EndRead(s1) {
					goto retry
				}
				if !opt.EmbeddedHash_ || e.GetHash() == hash {
					if e.key == key {
						return e.value, true
					}
				}
			}
		} else {
			// -race:
			// To use a Buffer & Scan approach for strict locking compatibility.
			// This prevents Double-Free / Use-After-Free bugs in strict mode
			// that occurred with the previous iterative loop.
			var cache [entriesPerBucket]entry_[K, V]
			var cacheCount int
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				cache[cacheCount] = b.At(j).ReadUnfenced()
				cacheCount++
			}
			if !b.seq.EndRead(s1) {
				goto retry
			}
			for i := range cacheCount {
				e := &cache[i]
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
		b = (*flatBucket[K, V])(loadPtr(&b.next))
	}
}

// Store sets the value for a key.
func (m *FlatMap[K, V]) Store(key K, value V) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets.ptr == nil {
		m.slowInit()
		table = SeqLockRead32(&m.tableSeq, &m.table)
	}

	var hash uintptr
	var h1v int
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(&key)))
		h1v = h1IntKey(hash)
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h1v = h1(hash)
	}
	h2v := h2(hash)
	h2w := broadcast(h2v)

	// Fast path: lock-free read
	{
		idx := table.mask & h1v
		b := table.buckets.At(idx)
		for {
			s1, ok := b.seq.BeginRead()
			if !ok {
				goto slowPath
			}
			meta := loadUint64Fast(&b.meta)

			//goland:noinspection GoBoolExpressions
			if !opt.Race_ {
				for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j).ReadUnfenced()
					if !b.seq.EndRead(s1) {
						goto slowPath
					}
					if !opt.EmbeddedHash_ || e.GetHash() == hash {
						if e.key == key {
							// valEqual: skip write if value unchanged
							if m.valEqual != nil {
								if m.valEqual(
									noescape(unsafe.Pointer(&e.value)),
									noescape(unsafe.Pointer(&value)),
								) {
									return
								}
							}
							goto slowPath
						}
					}
				}
			} else {
				var cache [entriesPerBucket]entry_[K, V]
				var cacheCount int
				for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					cache[cacheCount] = b.At(j).ReadUnfenced()
					cacheCount++
				}
				if !b.seq.EndRead(s1) {
					goto slowPath
				}
				for i := range cacheCount {
					e := &cache[i]
					if !opt.EmbeddedHash_ || e.GetHash() == hash {
						if e.key == key {
							// valEqual: skip write if value unchanged
							if m.valEqual != nil {
								if m.valEqual(
									noescape(unsafe.Pointer(&e.value)),
									noescape(unsafe.Pointer(&value)),
								) {
									return
								}
							}
							goto slowPath
						}
					}
				}
			}
			if meta&opNextMask == 0 {
				break
			}
			b = (*flatBucket[K, V])(loadPtr(&b.next))
		}
	}

slowPath:

	for {
		idx := table.mask & h1v
		root := table.buckets.At(idx)
		root.Lock()

		// Help finishing rebuild if needed
		if rs := (*flatRebuildState[K, V])(loadPtr(&m.rs)); rs != nil {
			switch rs.hint {
			case mapGrowHint, mapShrinkHint:
				if rs.newTableSeq.Ready() {
					root.Unlock()
					m.helpCopyAndWait(rs)
					table = SeqLockRead32(&m.tableSeq, &m.table)
					continue
				}
			case mapRebuildBlockWritersHint:
				root.Unlock()
				rs.latch.Wait()
				table = SeqLockRead32(&m.tableSeq, &m.table)
				continue
			default:
				// mapRebuildWithWritersHint: allow concurrent writers
			}
		}

		if newTable := SeqLockRead32(&m.tableSeq, &m.table); newTable.buckets.ptr != table.buckets.ptr {
			root.Unlock()
			table = newTable
			continue
		}

		var (
			oldB      *flatBucket[K, V]
			oldIdx    int
			emptyB    *flatBucket[K, V]
			emptyIdx  int
			emptyMeta uint64
		)

		b := root
	findLoop:
		for {
			meta := loadUint64Fast(&b.meta)
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).Ptr()
				if !opt.EmbeddedHash_ || e.GetHash() == hash {
					if e.key == key {
						oldB, oldIdx = b, j
						break findLoop
					}
				}
			}
			if emptyB == nil {
				if empty := (^meta) & metaMask; empty != 0 {
					emptyB = b
					emptyIdx = firstMarkedByteIndex(empty)
					emptyMeta = meta
				}
			}

			if meta&opNextMask == 0 {
				break
			}
			b = (*flatBucket[K, V])(b.next)
		}

		if oldB != nil {
			// Update
			newEnt := entry_[K, V]{key: key, value: value}
			if opt.EmbeddedHash_ {
				newEnt.SetHash(hash)
			}
			e := oldB.At(oldIdx)
			oldB.seq.BeginWriteLocked()
			e.WriteUnfenced(newEnt)
			oldB.seq.EndWriteLocked()
			root.Unlock()
			return
		}

		newEnt := entry_[K, V]{key: key, value: value}
		if opt.EmbeddedHash_ {
			newEnt.SetHash(hash)
		}

		// Insert into empty slot
		if emptyB != nil {
			// insert new: no seqlock window needed since slot was empty.
			// Reader won't access slot until meta is published with valid h2.
			emptyB.At(emptyIdx).WriteUnfenced(newEnt)
			newMeta := setByte(emptyMeta, h2v, emptyIdx)
			storeUint64(&emptyB.meta, newMeta)

			root.Unlock()
			table.AddSize(idx, 1)
			return
		}

		// append new bucket
		storePtr(&b.next, unsafe.Pointer(&flatBucket[K, V]{
			meta: setByte(emptyMeta, h2v, 0),
			entries: [entriesPerBucket]SeqLockSlot[entry_[K, V]]{
				{buf: newEnt},
			},
		}))
		newMeta := loadUint64Fast(&b.meta) | opNextMask
		if b == root {
			root.UnlockWithMeta(newMeta)
		} else {
			storeUint64(&b.meta, newMeta)
			root.Unlock()
		}
		table.AddSize(idx, 1)

		// Auto-grow check (parallel resize)
		if loadPtr(&m.rs) == nil {
			tableLen := table.mask + 1
			size := table.SumSize()
			const capFactor = float64(entriesPerBucket) * loadFactor
			if size >= int(float64(tableLen)*capFactor) {
				m.tryResize(mapGrowHint, size, 0)
			}
		}
		return
	}
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMap[K, V]) LoadOrStore(
	key K,
	value V,
) (actual V, loaded bool) {
	return m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(value)
	}, computeInit|computeSkipIfFound)
}

// LoadOrStoreFn loads the value for a key if present.
// Otherwise, it stores and returns the value returned by valueFn.
// The loaded result is true if the value was loaded, false if stored.
func (m *FlatMap[K, V]) LoadOrStoreFn(
	key K,
	valueFn func() V,
) (actual V, loaded bool) {
	return m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(valueFn())
	}, computeInit|computeSkipIfFound)
}

// LoadAndUpdate updates the value for key if it exists, returning the previous
// value. The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	_, loaded = m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			previous = e.Value()
			e.Update(value)
		}
	}, computeSkipIfNotFound)
	return previous, loaded
}

// LoadAndDelete deletes the value for a key, returning the previous value.
// The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	_, loaded = m.compute(&key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			previous = e.Value()
			e.Delete()
		}
	}, computeSkipIfNotFound)
	return previous, loaded
}

// Swap stores value for key and returns the previous value if any.
// The loaded result reports whether the key was present.
func (m *FlatMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	_, loaded = m.compute(&key, func(e *MapEntry[K, V]) {
		previous = e.Value()
		e.Update(value)
	}, computeInit)
	return previous, loaded
}

// Delete deletes the value for a key.
func (m *FlatMap[K, V]) Delete(key K) {
	m.compute(&key, func(e *MapEntry[K, V]) {
		e.Delete()
	}, computeSkipIfNotFound)
}

// CompareAndSwap atomically replaces an existing value with a new value.
// If the existing value matches the expected value.
func (m *FlatMap[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets.ptr == nil {
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
// If its value matches the expected value.
func (m *FlatMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets.ptr == nil {
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
//   - actual: the current value in the map after the operation
//   - loaded: true if the key existed before the operation
func (m *FlatMap[K, V]) Compute(
	key K,
	fn func(e *MapEntry[K, V]),
) (actual V, loaded bool) {
	return m.compute(&key, fn, computeInit)
}

func (m *FlatMap[K, V]) compute(
	key *K,
	fn func(e *MapEntry[K, V]),
	flags uint8,
) (actual V, loaded bool) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets.ptr == nil {
		if flags&computeInit == 0 {
			return *new(V), false
		}
		m.slowInit()
		table = SeqLockRead32(&m.tableSeq, &m.table)
	}

	var hash uintptr
	var h1v int
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(key)))
		h1v = h1IntKey(hash)
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
		for {
			s1, ok := b.seq.BeginRead()
			if !ok {
				goto slowPath
			}
			meta := loadUint64Fast(&b.meta)

			//goland:noinspection GoBoolExpressions
			if !opt.Race_ {
				for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j).ReadUnfenced()
					if !b.seq.EndRead(s1) {
						goto slowPath
					}
					if !opt.EmbeddedHash_ || e.GetHash() == hash {
						if e.key == *key {
							if flags&computeSkipIfFound != 0 {
								return e.value, true
							}
							goto slowPath
						}
					}
				}
			} else {
				var cache [entriesPerBucket]entry_[K, V]
				var cacheCount int
				for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					cache[cacheCount] = b.At(j).ReadUnfenced()
					cacheCount++
				}
				if !b.seq.EndRead(s1) {
					goto slowPath
				}
				for i := range cacheCount {
					e := &cache[i]
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
			b = (*flatBucket[K, V])(loadPtr(&b.next))
		}
		// Key not found in fast path
		if flags&computeSkipIfNotFound != 0 {
			return *new(V), false
		}
	}

slowPath:

	for {
		idx := table.mask & h1v
		root := table.buckets.At(idx)
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
						continue
					}
				case mapRebuildBlockWritersHint:
					root.Unlock()
					rs.latch.Wait()
					table = SeqLockRead32(&m.tableSeq, &m.table)
					continue
				default:
					// mapRebuildWithWritersHint: allow concurrent writers
				}
			}
		}
		if newTable := SeqLockRead32(&m.tableSeq, &m.table); newTable.buckets.ptr != table.buckets.ptr {
			root.Unlock()
			table = newTable
			continue
		}

		var (
			oldB      *flatBucket[K, V]
			oldIdx    int
			oldMeta   uint64
			emptyB    *flatBucket[K, V]
			emptyIdx  int
			emptyMeta uint64
		)
		it := MapEntry[K, V]{entry: entry_[K, V]{key: *key}}
		if opt.EmbeddedHash_ {
			it.entry.SetHash(hash)
		}

		b := root
	findLoop:
		for {
			meta := loadUint64Fast(&b.meta)
			for marked := markZeroBytes(meta ^ h2w); marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				e := b.At(j).Ptr()
				if !opt.EmbeddedHash_ || e.GetHash() == hash {
					if e.key == *key {
						oldB, oldIdx, oldMeta, it.entry.value, it.loaded = b, j, meta, e.value, true
						break findLoop
					}
				}
			}
			if emptyB == nil {
				if empty := (^meta) & metaMask; empty != 0 {
					emptyB = b
					emptyIdx = firstMarkedByteIndex(empty)
					emptyMeta = meta
				}
			}
			if meta&opNextMask == 0 {
				break
			}
			b = (*flatBucket[K, V])(b.next)
		}

		fn(noEscape(&it))
		switch it.op {
		case updateOp:
			if it.loaded {
				// valEqual: skip write if value unchanged
				if m.valEqual != nil {
					if m.valEqual(
						noescape(unsafe.Pointer(&oldB.At(oldIdx).Ptr().value)),
						noescape(unsafe.Pointer(&it.entry.value)),
					) {
						root.Unlock()
						return it.entry.value, it.loaded
					}
				}
				// Update
				e := oldB.At(oldIdx)
				oldB.seq.BeginWriteLocked()
				e.WriteUnfenced(it.entry)
				oldB.seq.EndWriteLocked()
				root.Unlock()
				return it.entry.value, it.loaded
			}
			// Insert into empty slot
			if emptyB != nil {
				// insert new: no seqlock window needed since slot was empty.
				// Reader won't access slot until meta is published with valid h2.
				// StoreBarrier ensures Entry is visible before meta update on ARM.
				emptyB.At(emptyIdx).WriteUnfenced(it.entry)
				// emptyB.seq.WriteBarrier()
				newMeta := setByte(emptyMeta, h2v, emptyIdx)
				storeUint64(&emptyB.meta, newMeta)

				root.Unlock()
				table.AddSize(idx, 1)
				return it.entry.value, it.loaded
			}
			// append new bucket
			storePtr(&b.next, unsafe.Pointer(&flatBucket[K, V]{
				meta: setByte(emptyMeta, h2v, 0),
				entries: [entriesPerBucket]SeqLockSlot[entry_[K, V]]{
					{buf: it.entry},
				},
			}))
			newMeta := loadUint64Fast(&b.meta) | opNextMask
			if b == root {
				root.UnlockWithMeta(newMeta)
			} else {
				storeUint64(&b.meta, newMeta)
				root.Unlock()
			}
			table.AddSize(idx, 1)
			// Auto-grow check (parallel resize)
			if loadPtr(&m.rs) == nil {
				tableLen := table.mask + 1
				size := table.SumSize()
				const capFactor = float64(entriesPerBucket) * loadFactor
				if size >= int(float64(tableLen)*capFactor) {
					m.tryResize(mapGrowHint, size, 0)
				}
			}
			return it.entry.value, it.loaded
		case deleteOp:
			if !it.loaded {
				root.Unlock()
				return it.entry.value, it.loaded
			}
			// Delete: update meta first so new Readers skip this slot immediately.
			// Active Readers will see seq change and retry, then see h2=0.
			newMeta := setByte(oldMeta, h2Empty, oldIdx)
			storeUint64(&oldB.meta, newMeta)
			oldB.seq.BeginWriteLocked()
			oldB.At(oldIdx).WriteUnfenced(entry_[K, V]{})
			oldB.seq.EndWriteLocked()

			root.Unlock()
			table.AddSize(idx, -1)
			// Check if table shrinking is needed
			if m.shrinkOn {
				if newMeta&metaDataMask == metaEmpty {
					if loadPtr(&m.rs) == nil {
						tableLen := table.mask + 1
						if minTableLen < tableLen {
							size := table.SumSize()
							if size < tableLen*entriesPerBucket/shrinkFraction {
								m.tryResize(mapShrinkHint, size, 0)
							}
						}
					}
				}
			}
			return it.entry.value, it.loaded
		default:
			// cancelOp: No-op
			root.Unlock()
			return it.entry.value, it.loaded
		}
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
	if table.buckets.ptr == nil {
		return
	}

	var meta uint64
	var cache [entriesPerBucket]entry_[K, V]
	var cacheCount int
	for i := 0; i <= table.mask; i++ {
		b := table.buckets.At(i)
		for {
			var spins int
		retry:
			s1, ok := b.seq.BeginRead()

			if !ok {
				delay(&spins)
				goto retry
			}
			meta = loadUint64Fast(&b.meta)
			cacheCount = 0
			for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
				j := firstMarkedByteIndex(marked)
				cache[cacheCount] = b.At(j).ReadUnfenced()
				cacheCount++
			}
			if !b.seq.EndRead(s1) {
				goto retry
			}
			for j := range cacheCount {
				kv := &cache[j]
				if !yield(kv.key, kv.value) {
					return
				}
			}
			if meta&opNextMask == 0 {
				break
			}
			b = (*flatBucket[K, V])(loadPtr(&b.next))
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

// ComputeRange iterates all entries and applies a user callback.
//
// Callback signature:
//
//		fn(e *MapEntry[K, V]) bool
//
//	  - e.Update(newV): update the entry to newV
//	  - e.Delete(): delete the entry
//	  - default (no op): keep the entry unchanged
//	  - return true to continue; return false to stop iteration
//
// Concurrency & consistency:
//   - Cooperates with concurrent grow/shrink; if a resize is detected, it
//     helps complete copying, then continues on the latest table.
//   - Holds the root-bucket lock while processing its bucket chain to
//     coordinate with writers/resize operations.
//
// Parameters:
//   - fn: user function applied to each key-value pair.
//   - blockWriters: optional flag (default false). If true, concurrent writers
//     are blocked during iteration; resize operations are always exclusive.
//
// Recommendation: keep fn lightweight to reduce lock hold time.
func (m *FlatMap[K, V]) ComputeRange(
	fn func(e *MapEntry[K, V]) bool,
	blockWriters ...bool,
) {
	hint := mapRebuildAllowWritersHint
	if len(blockWriters) != 0 && blockWriters[0] {
		hint = mapRebuildBlockWritersHint
	}

	m.rebuild(hint, func(_ *MapRebuild[K, V]) {
		table := SeqLockRead32(&m.tableSeq, &m.table)
		if table.buckets.ptr == nil {
			return
		}
		it := MapEntry[K, V]{
			loaded: true,
		}
		for i := 0; i <= table.mask; i++ {
			root := table.buckets.At(i)
			root.Lock()
			b := root
			for {
				meta := loadUint64Fast(&b.meta)
				for marked := meta & metaMask; marked != 0; marked &= marked - 1 {
					j := firstMarkedByteIndex(marked)
					e := b.At(j)
					it.entry = *e.Ptr()
					it.op = cancelOp
					shouldContinue := fn(noEscape(&it))
					switch it.op {
					case updateOp:
						b.seq.BeginWriteLocked()
						e.WriteUnfenced(it.entry)
						b.seq.EndWriteLocked()
					case deleteOp:
						meta = setByte(meta, h2Empty, j)
						storeUint64(&b.meta, meta)
						b.seq.BeginWriteLocked()
						e.WriteUnfenced(entry_[K, V]{})
						b.seq.EndWriteLocked()
						table.AddSize(i, -1)
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
				b = (*flatBucket[K, V])(b.next)
			}
			root.Unlock()
		}
	})
}

// Entries returns an iterator function for use with range-over-func.
// It provides the same functionality as ComputeRange but in iterator form.
//
//go:nosplit
func (m *FlatMap[K, V]) Entries(
	blockWriters ...bool,
) func(yield func(e *MapEntry[K, V]) bool) {
	return func(yield func(e *MapEntry[K, V]) bool) {
		m.ComputeRange(yield, blockWriters...)
	}
}

// Size returns the number of key-value pairs in the map.
// This operation sums counters across all size stripes for an approximate
// count.
func (m *FlatMap[K, V]) Size() int {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets.ptr == nil {
		return 0
	}

	return table.SumSize()
}

// Clear clears all key-value pairs from the map.
func (m *FlatMap[K, V]) Clear() {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets.ptr == nil {
		return
	}

	m.rebuild(mapRebuildBlockWritersHint, func(_ *MapRebuild[K, V]) {
		SeqLockWriteLocked32(&m.tableSeq, &m.table,
			newFlatTable[K, V](minTableLen, maxProcs()))
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
	if table.buckets.ptr == nil {
		m.slowInit()
	}
	m.doResize(mapGrowHint, sizeAdd)
}

// Shrink reduces the capacity to fit the current size,
// always executes regardless of WithAutoShrink.
func (m *FlatMap[K, V]) Shrink() {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	if table.buckets.ptr == nil {
		return
	}
	m.doResize(mapShrinkHint, -1)
}

func (m *FlatMap[K, V]) doResize(hint mapRebuildHint, sizeAdd int) {
	var size int
	for {
		// Resize check
		table := SeqLockRead32(&m.tableSeq, &m.table)
		tableLen := table.mask + 1
		if hint == mapGrowHint {
			if sizeAdd <= 0 {
				return
			}
			size = table.SumSize()
			newTableLen := calcTableLen(size + sizeAdd)
			if tableLen >= newTableLen {
				return
			}
		} else {
			// mapShrinkHint
			if tableLen <= minTableLen {
				return
			}
			// Recalculate the shrink size to avoid over-shrinking
			size = table.SumSize()
			newTableLen := calcTableLen(size)
			if tableLen <= newTableLen {
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
		m.tryResize(hint, size, sizeAdd)
	}
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
	if table.buckets.ptr == nil {
		return
	}
	clone.seed = m.seed
	clone.keyHash = m.keyHash
	clone.valEqual = m.valEqual
	clone.shrinkOn = m.shrinkOn
	clone.intKey = m.intKey
	SeqLockWriteLocked32(&clone.tableSeq, &clone.table,
		newFlatTable[K, V](minTableLen, maxProcs()))
	clone.Grow(m.Size())
	for k, v := range m.All() {
		clone.Store(k, v)
	}
}

// Rebuild performs a map rebuild operation with the given function.
// The function is executed with exclusive access
// (or shared based on blockWriters) to the map.
//
// Parameters:
//   - fn: The function to execute during rebuild.
//     It receives a MapRebuild instance.
//   - blockWriters: Optional. If true, concurrent writers are blocked.
//     Default is false (allow writers).
//
// Notes:
//   - You must use the `m *MapRebuild[K, V]` parameter passed to `fn` for
//     processing. Do not call methods on the Map instance directly, as this
//     may cause deadlocks.
func (m *FlatMap[K, V]) Rebuild(
	fn func(m *MapRebuild[K, V]),
	blockWriters ...bool,
) {
	hint := mapRebuildAllowWritersHint
	if len(blockWriters) != 0 && blockWriters[0] {
		hint = mapRebuildBlockWritersHint
	}

	m.rebuild(hint, fn)
}

// rebuild reorganizes the map. Only these hints are supported:
//   - mapRebuildWithWritersHint: allows concurrent reads/writes
//   - mapExclusiveRebuildHint: allows concurrent reads
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
		if rs, ok := m.beginRebuild(rebuildHint); ok {
			fn(noEscape(&MapRebuild[K, V]{f: m}))
			m.endRebuild(rs)
			return
		}
	}
}

func (m *FlatMap[K, V]) beginRebuild(hint mapRebuildHint) (*flatRebuildState[K, V], bool) {
	if loadPtr(&m.rs) != nil {
		return nil, false
	}
	rs := &flatRebuildState[K, V]{
		hint: hint,
	}
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return nil, false
	}
	return rs, true
}

func (m *FlatMap[K, V]) endRebuild(rs *flatRebuildState[K, V]) {
	atomic.StorePointer(&m.rs, nil)
	rs.latch.Open()
}

//go:noinline
func (m *FlatMap[K, V]) tryResize(hint mapRebuildHint, size, sizeAdd int) {
	// Inline m.beginRebuild(hint)
	if loadPtr(&m.rs) != nil {
		return
	}
	rs := &flatRebuildState[K, V]{hint: hint}
	if !atomic.CompareAndSwapPointer(&m.rs, nil, unsafe.Pointer(rs)) {
		return
	}

	table := m.table.Ptr()
	tableLen := table.mask + 1
	var newLen int
	if hint == mapGrowHint {
		if sizeAdd == 0 {
			newLen = tableLen << 1 // max(calcTableLen(size), tableLen<<1)
		} else {
			newLen = calcTableLen(size + sizeAdd)
			if newLen <= tableLen {
				m.endRebuild(rs)
				return
			}
		}
	} else {
		// mapShrinkHint
		if sizeAdd == 0 {
			newLen = tableLen >> 1
			if newLen < minTableLen {
				m.endRebuild(rs)
				return
			}
		} else {
			newLen = calcTableLen(size)
			if newLen >= tableLen {
				m.endRebuild(rs)
				return
			}
		}
	}

	cpus := maxProcs()
	if newLen*int(unsafe.Sizeof(flatBucket[K, V]{})) >= asyncThreshold || cpus <= 1 {
		chunks := calcParallelism(table.mask+1, minBucketsPerCPU, cpus*resizeOverPartition)
		rs.chunks.Store(int32(chunks))
		// rs.process.Store(0)
		// rs.completed.Store(0)
		// rs.newTableSeq.ClearLocked()
		// Inline newFlatTable
		sizeLen := calcSizeLen(newLen, cpus)
		SeqLockWriteLocked32(&rs.newTableSeq, &rs.newTable, flatTable[K, V]{
			buckets:  makeUnsafeSlice[flatBucket[K, V]](newLen),
			mask:     newLen - 1,
			size:     makeUnsafeSlice[counterStripe](sizeLen),
			sizeMask: sizeLen - 1,
		})
		m.helpCopyAndWait(rs)

	} else {
		go func(
			table *flatTable[K, V],
			newLen int,
			rs *flatRebuildState[K, V],
			cpus int,
		) {
			chunks := calcParallelism(table.mask+1, minBucketsPerCPU, cpus*resizeOverPartition)
			rs.chunks.Store(int32(chunks))
			// rs.process.Store(0)
			// rs.completed.Store(0)
			// rs.newTableSeq.ClearLocked()
			// Inline newFlatTable
			sizeLen := calcSizeLen(newLen, cpus)
			SeqLockWriteLocked32(&rs.newTableSeq, &rs.newTable, flatTable[K, V]{
				buckets:  makeUnsafeSlice[flatBucket[K, V]](newLen),
				mask:     newLen - 1,
				size:     makeUnsafeSlice[counterStripe](sizeLen),
				sizeMask: sizeLen - 1,
			})
			m.helpCopyAndWait(rs)
		}(table, newLen, rs, cpus)
	}
}

//go:noinline
func (m *FlatMap[K, V]) helpCopyAndWait(rs *flatRebuildState[K, V]) {
	table := SeqLockRead32(&m.tableSeq, &m.table)
	newTable := SeqLockRead32(&rs.newTableSeq, &rs.newTable)
	if newTable.buckets.ptr == nil ||
		newTable.buckets.ptr == table.buckets.ptr {
		return
	}
	chunks := rs.chunks.Load()
	if chunks == 0 {
		return
	}
	oldLen := table.mask + 1
	newLen := newTable.mask + 1
	// Determines the concurrent task range for destination buckets.
	// We iterate based on the "Destination Constraint" to allow lock-free
	// writes:
	// - Grow (Pow2):   baseLen == oldLen. Source i moves to Dest i, i+baseLen...
	// - Shrink (Pow2): baseLen == newLen. Source i, i+baseLen... move to Dest i.
	// By iterating 0..baseLen and processing all aliasing source buckets
	// (srcIdx += baseLen) in the inner loop, a single goroutine exclusively
	// owns the write operations for its assigned destination buckets.
	baseLen := min(newLen, oldLen)
	chunkSz := (baseLen + int(chunks) - 1) / int(chunks)
	for {
		process := rs.process.Add(1)
		if process > chunks {
			rs.latch.Wait()
			return
		}
		process--
		start := int(process) * chunkSz
		end := min(start+chunkSz, baseLen)
		m.copyBucket(&table, start, end, oldLen, baseLen, &newTable)
		if rs.completed.Add(1) == chunks {
			SeqLockWriteLocked32(&m.tableSeq, &m.table, newTable)
			// rs.newTableSeq.ClearLocked()
			m.endRebuild(rs)
			return
		}
	}
}

func (m *FlatMap[K, V]) copyBucket(
	table *flatTable[K, V],
	start, end int,
	oldLen, baseLen int,
	newTable *flatTable[K, V],
) {
	mask := newTable.mask
	copied := 0
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
					e := b.At(j).Ptr()
					var hash uintptr
					var h1v int
					if opt.EmbeddedHash_ {
						hash = e.GetHash()
						if m.intKey {
							h1v = h1IntKey(hash)
						} else {
							h1v = h1(hash)
						}
					} else {
						if m.intKey {
							hash = intHash[K](noescape(unsafe.Pointer(&e.key)))
							h1v = h1IntKey(hash)
						} else {
							hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
							h1v = h1(hash)
						}
					}
					idx := mask & h1v
					destB := newTable.buckets.At(idx)
					h2v := h2(hash)
					// Append entry to the destination bucket
					for {
						meta := destB.meta
						if empty := (^meta) & metaMask; empty != 0 {
							emptyIdx := firstMarkedByteIndex(empty)
							destB.meta = setByte(meta, h2v, emptyIdx)
							*destB.At(emptyIdx).Ptr() = *e
							break
						}
						next := (*flatBucket[K, V])(destB.next)
						if next == nil {
							destB.next = unsafe.Pointer(&flatBucket[K, V]{
								meta: setByte(metaEmpty, h2v, 0),
								entries: [entriesPerBucket]SeqLockSlot[entry_[K, V]]{
									{buf: *e},
								},
							})
							destB.meta = meta | opNextMask
							break
						}
						destB = next
					}
					copied++
				}
				if meta&opNextMask == 0 {
					break
				}
				b = (*flatBucket[K, V])(b.next)
			}
			srcB.Unlock()
		}
	}
	if copied != 0 {
		newTable.AddSize(start, copied)
	}
}

func newFlatTable[K comparable, V any](
	tableLen, cpus int,
) flatTable[K, V] {
	sizeLen := calcSizeLen(tableLen, cpus)
	return flatTable[K, V]{
		buckets:  makeUnsafeSlice[flatBucket[K, V]](tableLen),
		mask:     tableLen - 1,
		size:     makeUnsafeSlice[counterStripe](sizeLen),
		sizeMask: sizeLen - 1,
	}
}

//go:nosplit
func (t *flatTable[K, V]) AddSize(idx, delta int) {
	atomic.AddUintptr(&t.size.At(t.sizeMask&idx).c, uintptr(delta))
}

//go:nosplit
func (t *flatTable[K, V]) SumSize() int {
	var sum uintptr
	for i := 0; i <= t.sizeMask; i++ {
		sum += loadUintptr(&t.size.At(i).c)
	}
	return int(sum)
}

//go:nosplit
func (b *flatBucket[K, V]) At(i int) *SeqLockSlot[entry_[K, V]] {
	return (*SeqLockSlot[entry_[K, V]])(unsafe.Add(
		unsafe.Pointer(&b.entries),
		uintptr(i)*unsafe.Sizeof(SeqLockSlot[entry_[K, V]]{}),
	))
}

func (b *flatBucket[K, V]) Lock() {
	// Inline BitLockUint64(&b.meta, opLockMask)
	cur := atomic.LoadUint64(&b.meta)
	if !atomic.CompareAndSwapUint64(&b.meta, cur&^opLockMask, cur|opLockMask) {
		slowBitLockUint64(&b.meta, opLockMask)
	}
}

//go:nosplit
func (b *flatBucket[K, V]) Unlock() {
	BitUnlockUint64(&b.meta, opLockMask)
}

//go:nosplit
func (b *flatBucket[K, V]) UnlockWithMeta(meta uint64) {
	BitUnlockWithStoreUint64(&b.meta, opLockMask, meta)
}
