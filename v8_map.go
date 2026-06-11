//go:build !race

package cc

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	// v8EnableIntKey enables the specialized integer hash/start path.
	v8EnableIntKey = true
	// v8EnableDedupVal skips publishing an update when the new value equals
	// the existing value and a value equality function is available.
	v8EnableDedupVal = true
	// v8EnableStoreInGrow lets writers keep retrying against the old table
	// while a resize leader is allocating/publishing the next table. Keep it
	// off by default so writers join cooperative resize instead of extending
	// contention on old buckets.
	v8EnableStoreInGrow = false
	// v8EnableAggressiveGrow adds one extra size class on probe-limit resize
	// or observed concurrent insert pressure during table allocation.
	v8EnableAggressiveGrow = true
	// v8EnableSameKeyTombstoneReuse lets a Store revive a tombstone left by
	// the same key. When disabled, deletes clear both key and value while the
	// tombstone remains as a probe-continuation marker until resize compaction.
	v8EnableSameKeyTombstoneReuse = true
)

const (
	v8MinBuckets     = 32
	v8SlotsPerBucket = 8
	v8LoadFactorNum  = 13
	v8LoadFactorDen  = 16
	v8LaneMarkerMask = uint64(0x8080808080808080)
)

const (
	v8TagEmpty   = uint8(0)
	v8TagDeleted = uint8(1)
)

const (
	// Keep ctrl for version/frozen/writing only. If miss-heavy workloads need a
	// probe-continuation hint, prefer a side structure so displaced inserts do not
	// have to update shared bucket metadata.
	v8WritingMask = uint32(1) << 0
	v8FrozenMask  = uint32(1) << 1
	v8VersionMask = uint32(0xFFFFFFFC)
	v8VersionInc  = uint32(1) << 2
)

const (
	// occupied tracks physical lanes that are no longer Empty: Full + Tombstone.
	// tombstones tracks lanes currently in Deleted/Tombstone state, not total
	// delete operations.
	v8CntOccupied = iota
	v8CntTombstones
)

type v8Status uint8

const (
	v8OK v8Status = iota
	v8Full
	v8Retry
	v8Frozen
)

type v8ResizeHint uint8

const (
	v8ResizeNormal v8ResizeHint = iota
	v8ResizeProbeLimit
	v8ResizeCompact
)

// V8Map is an experimental SWAR-probed open-addressed map.
//
// Each bucket stores four one-byte tags plus a compact control word. Entries
// are kept in a separate flat array and addressed by bucket/lane, so probing
// stays compact while key/value storage remains contiguous. Reads use a bucket
// version snapshot; writes publish through a short per-bucket writing window,
// and resize freezes old buckets cooperatively. Like [OFHTMap] and [FlatMap],
// entry payloads are copied through SeqLockSlot so weak-memory architectures
// do not move large key/value reads outside the version-checked window.
//
// Integer keys use the fast integer hash path. Other comparable key shapes
// keep Go's built-in hasher to preserve == semantics. Use [WithKeyHasherUnsafe]
// to supply a custom hasher for custom key types.
type V8Map[K comparable, V any] struct {
	_         noCopy
	table     atomic.Pointer[v8Table[K, V]]
	initState atomic.Uint32
	intKey    bool
	seed      uintptr
	keyHash   HashFunc
	valEqual  EqualFunc
	minLen    uintptr
	size      PLocalCounterN
}

type v8Table[K comparable, V any] struct {
	buckets       unsafeSlice[v8Bucket]
	entries       unsafeSlice[SeqLockSlot[v8Entry[K, V]]]
	mask          uintptr
	probeLimit    uintptr
	stripeCap     int
	growCap       int
	intKey        bool
	chunks        uint32
	chunkSz       uintptr
	allocating    atomic.Uint32
	copyIdx       atomic.Uint32
	copyDone      atomic.Uint32
	copyMaxProbe  atomic.Uintptr
	nextTable     atomic.Pointer[v8Table[K, V]]
	bucketBacking unsafe.Pointer
}

// v8Bucket stores the tags and control word for a bucket.
// Layout: 8 bytes for 8 slots of tags, 4 bytes for control metadata.
// Total useful data: 12 bytes.
// Alignment: 8-byte aligned (due to atomic.Uint64).
// Padding: 4 bytes of implicit padding at the end to satisfy the 8-byte
// alignment, bringing the total struct size to 16 bytes.
//
// ┌───────────────────────────────────────────────────────────────────┐
// │                    8 × 8-bit h2 tags (64 bits)                    │
// │                             bytes 0-7                             │
// ├───────────────────┬──────┬──────┬─────────────────────────────────┤
// │ version (30 bits) │frozen│ write│         padding (4 bytes)       │
// │     bits 31-2     │bit 1 │ bit 0│             bytes 12-15         │
// └───────────────────┴──────┴──────┴─────────────────────────────────┘
type v8Bucket struct {
	tags atomic.Uint64 // [8] bytes of tag
	ctrl atomic.Uint32
}

type v8Entry[K comparable, V any] struct {
	key K
	val V
}

func NewV8Map[K comparable, V any](options ...func(*MapConfig)) *V8Map[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m := &V8Map[K, V]{}
	m.init(noEscape(&cfg))
	return m
}

func (m *V8Map[K, V]) init(cfg *MapConfig) {
	if cfg.keyHash == nil {
		cfg.keyHash = parseKeyInterface[K]()
	}
	if cfg.valEqual == nil {
		cfg.valEqual = parseValueInterface[V]()
	}
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	m.intKey = v8EnableIntKey && m.intKey
	if cfg.keyHash != nil {
		m.keyHash = cfg.keyHash
		m.intKey = false
	}
	if cfg.valEqual != nil {
		m.valEqual = cfg.valEqual
	}
	m.seed = uintptr(rand.Uint64())
	m.minLen = v8CalcBucketLen(cfg.capacity)
	m.table.Store(newV8Table[K, V](m.minLen, m.intKey))
}

func (m *V8Map[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	hash := m.hashKey(noEscape(&key))
	tag, start := v8HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
		// spins := 0
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v8WritingMask != 0 {
			// delay(&spins)
			goto retryBucket
		}
		words := v8LoadTagWords(b)
		match := v8MatchBits(words, tag)
		for match != 0 {
			lane := v8FirstMarkedLane(match)
			e := table.entry(bi, lane).ReadUnfenced()
			ctrl2 := b.ctrl.Load()
			if ctrl != ctrl2 {
				goto retryBucket
			}
			if e.key == key {
				return e.val, true
			}
			match &= match - 1
		}
		if v8EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return *new(V), false
		}
	}
	return *new(V), false
}

func (m *V8Map[K, V]) Store(key K, value V) {
	m.store(noEscape(&key), noEscape(&value), false)
}

func (m *V8Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.store(noEscape(&key), noEscape(&value), true)
}

func (m *V8Map[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	return m.update(noEscape(&key), noEscape(&value), false)
}

func (m *V8Map[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.delete(noEscape(&key), true)
}

func (m *V8Map[K, V]) Delete(key K) {
	m.delete(noEscape(&key), false)
}

func (m *V8Map[K, V]) CompareAndSwap(key K, old V, new V) bool {
	table := m.table.Load()
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("cc: value is not comparable; use WithValueEqual")
	}
	hash := m.hashKey(noEscape(&key))
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			if table == nil {
				return false
			}
			continue
		}
		status, swapped := m.compareAndSwapIn(table, noEscape(&key), hash, noEscape(&old), noEscape(&new))
		switch status {
		case v8OK:
			return swapped
		case v8Frozen:
			table = m.helpResize(table)
		case v8Full:
			return false
		}
	}
}

func (m *V8Map[K, V]) CompareAndDelete(key K, old V) bool {
	table := m.table.Load()
	if table == nil {
		return false
	}
	if m.valEqual == nil {
		panic("cc: value is not comparable; use WithValueEqual")
	}
	hash := m.hashKey(noEscape(&key))
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			if table == nil {
				return false
			}
			continue
		}
		status, deleted := m.compareAndDeleteIn(table, noEscape(&key), hash, noEscape(&old))
		switch status {
		case v8OK:
			return deleted
		case v8Frozen:
			table = m.helpResize(table)
		case v8Full:
			return false
		}
	}
}

func (m *V8Map[K, V]) Compute(key K, fn func(e *MapEntry[K, V])) (actual V, loaded bool) {
	table := m.ensureTable()
	hash := m.hashKey(noEscape(&key))
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.computeIn(table, noEscape(&key), hash, fn)
		switch status {
		case v8OK:
			if !loaded && shouldCheckResize && int(m.size.Get(v8CntOccupied)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v8Full:
			table = m.tryResize(table, int(m.size.Value(v8CntOccupied)), v8ResizeProbeLimit)
		case v8Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V8Map[K, V]) Range(yield func(K, V) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}

	var cache [v8SlotsPerBucket]v8Entry[K, V]
	unsafeCache := toUnsafeSlice(&cache[0])
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
	retry:
		ctrl := b.ctrl.Load()
		if ctrl&v8WritingMask != 0 {
			goto retry
		}
		words := v8LoadTagWords(b)
		full := v8FullBits(words)
		var cacheCount uintptr
		for full != 0 {
			lane := v8FirstMarkedLane(full)
			*unsafeCache.At(cacheCount) = table.entry(i, lane).ReadUnfenced()
			cacheCount++
			full &= full - 1
		}
		ctrl2 := b.ctrl.Load()
		if ctrl != ctrl2 {
			goto retry
		}
		for j := range cacheCount {
			kv := unsafeCache.At(j)
			if !yield(kv.key, kv.val) {
				return
			}
		}
	}
}

func (m *V8Map[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

type v8MapStats struct {
	Buckets        uintptr
	Capacity       uintptr
	Live           uintptr
	Occupied       uintptr
	Tombstones     uintptr
	FullBuckets    uintptr
	FullLanes      uintptr
	EmptyLanes     uintptr
	TombstoneLanes uintptr
	MaxProbe       uintptr
	ProbeTotal     uintptr
	ProbeSamples   uintptr
}

func (m *V8Map[K, V]) stats() v8MapStats {
	table := m.table.Load()
	occupied := m.size.Value(v8CntOccupied)
	tombstones := m.size.Value(v8CntTombstones)
	stats := v8MapStats{
		Live:       occupied - tombstones,
		Occupied:   occupied,
		Tombstones: tombstones,
	}
	if table == nil {
		return stats
	}
	stats.Buckets = table.bucketLen()
	stats.Capacity = stats.Buckets * v8SlotsPerBucket
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
		words := v8LoadTagWords(b)
		full := v8FullBits(words)
		empty := v8EmptyBits(words)
		tombstones := v8DeletedBits(words)
		fullCount := uintptr(bits.OnesCount64(full))
		stats.FullLanes += fullCount
		stats.EmptyLanes += uintptr(bits.OnesCount64(empty))
		stats.TombstoneLanes += uintptr(bits.OnesCount64(tombstones))
		if fullCount == v8SlotsPerBucket {
			stats.FullBuckets++
		}
		fullScan := full
		for fullScan != 0 {
			lane := v8FirstMarkedLane(fullScan)
			e := table.entry(i, lane).ReadUnfenced()
			hash := m.hashKey(noEscape(&e.key))
			_, start := v8HashParts(hash, table.intKey, table.mask)
			probe := (i - start) & table.mask
			stats.ProbeTotal += probe
			stats.ProbeSamples++
			if probe > stats.MaxProbe {
				stats.MaxProbe = probe
			}
			fullScan &= fullScan - 1
		}
	}
	return stats
}

func (m *V8Map[K, V]) Size() int {
	occupied := int(m.size.Value(v8CntOccupied))
	tombstones := int(m.size.Value(v8CntTombstones))
	return max(occupied-tombstones, 0)
}

func (m *V8Map[K, V]) Clear() {
	if m.table.Load() == nil {
		return
	}
	m.size.Clear()
	m.table.Store(newV8Table[K, V](m.minLen, m.intKey))
}

func (m *V8Map[K, V]) store(key *K, val *V, onlyIfAbsent bool) (actual V, loaded bool) {
	table := m.ensureTable()
	hash := m.hashKey(key)
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.storeIn(table, key, val, hash, onlyIfAbsent)
		switch status {
		case v8OK:
			if !loaded && shouldCheckResize && int(m.size.Get(v8CntOccupied)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v8Full:
			table = m.tryResize(table, int(m.size.Value(v8CntOccupied)), v8ResizeProbeLimit)
		case v8Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V8Map[K, V]) update(key *K, val *V, onlyIfAbsent bool) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	hash := m.hashKey(key)
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			if table == nil {
				return *new(V), false
			}
			continue
		}
		status, previous, loaded, shouldCheckResize := m.updateIn(table, key, val, hash, onlyIfAbsent)
		switch status {
		case v8OK:
			if !loaded && shouldCheckResize && int(m.size.Get(v8CntOccupied)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return previous, loaded
		case v8Full:
			return *new(V), false
		case v8Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V8Map[K, V]) storeIn(
	table *v8Table[K, V],
	key *K,
	val *V,
	hash uintptr,
	onlyIfAbsent bool,
) (v8Status, V, bool, bool) {
	tag, start := v8HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v8FrozenMask != 0 {
			return v8Frozen, *new(V), false, false
		}
		if ctrl&v8WritingMask != 0 {
			goto retryBucket
		}
		words := v8LoadTagWords(b)
		match := v8MatchBits(words, tag)
		for match != 0 {
			lane := v8FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if onlyIfAbsent {
					return v8OK, e.val, true, false
				}
				if v8EnableDedupVal && m.valEqual != nil &&
					m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return v8OK, e.val, true, false
				}
				ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
				if status != v8OK {
					if status == v8Retry {
						goto retryBucket
					}
					return status, *new(V), false, false
				}
				slot.WriteUnfenced(v8Entry[K, V]{key: e.key, val: *val})
				v8EndWriteModified(b, ctrl)
				return v8OK, *val, true, false
			}
			match &= match - 1
		}
		if v8EnableSameKeyTombstoneReuse {
			tombstones := v8DeletedBits(words)
			for tombstones != 0 {
				lane := v8FirstMarkedLane(tombstones)
				slot := table.entry(bi, lane)
				e := slot.ReadUnfenced()
				if ctrl != b.ctrl.Load() {
					goto retryBucket
				}
				if e.key == *key {
					ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
					if status != v8OK {
						if status == v8Retry {
							goto retryBucket
						}
						return status, *new(V), false, false
					}
					slot.WriteUnfenced(v8Entry[K, V]{key: e.key, val: *val})
					v8StoreTag(b, lane, tag)
					v8EndWriteModified(b, ctrl)
					m.size.Add(v8CntTombstones, ^uintptr(0))
					return v8OK, *val, false, false
				}
				tombstones &= tombstones - 1
			}
		}
		if empty := v8EmptyBits(words); empty != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
			if status != v8OK {
				if status == v8Retry {
					goto retryBucket
				}
				return status, *new(V), false, false
			}
			lane := v8FirstMarkedLane(empty)
			table.entry(bi, lane).WriteUnfenced(v8Entry[K, V]{key: *key, val: *val})
			v8StoreTag(b, lane, tag)
			v8EndWriteModified(b, ctrl)
			m.size.Add(v8CntOccupied, 1)
			return v8OK, *val, false, empty&(empty-1) == 0
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v8Full, *new(V), false, false
}

func (m *V8Map[K, V]) updateIn(
	table *v8Table[K, V],
	key *K,
	val *V,
	hash uintptr,
	onlyIfAbsent bool,
) (v8Status, V, bool, bool) {
	tag, start := v8HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v8FrozenMask != 0 {
			return v8Frozen, *new(V), false, false
		}
		if ctrl&v8WritingMask != 0 {
			goto retryBucket
		}
		words := v8LoadTagWords(b)
		match := v8MatchBits(words, tag)
		for match != 0 {
			lane := v8FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if onlyIfAbsent {
					return v8OK, e.val, true, false
				}
				if v8EnableDedupVal && m.valEqual != nil &&
					m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return v8OK, e.val, true, false
				}
				ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
				if status != v8OK {
					if status == v8Retry {
						goto retryBucket
					}
					return status, *new(V), false, false
				}
				slot.WriteUnfenced(v8Entry[K, V]{key: e.key, val: *val})
				v8EndWriteModified(b, ctrl)
				return v8OK, e.val, true, false
			}
			match &= match - 1
		}
		if v8EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v8OK, *new(V), false, false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v8OK, *new(V), false, false
}

func (m *V8Map[K, V]) delete(key *K, needValue bool) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	hash := m.hashKey(key)
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			if table == nil {
				return *new(V), false
			}
			continue
		}
		status, previous, loaded := m.deleteIn(table, key, hash, needValue)
		switch status {
		case v8OK:
			return previous, loaded
		case v8Full:
			return *new(V), false
		case v8Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V8Map[K, V]) deleteIn(
	table *v8Table[K, V],
	key *K,
	hash uintptr,
	needValue bool,
) (v8Status, V, bool) {
	tag, start := v8HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v8FrozenMask != 0 {
			return v8Frozen, *new(V), false
		}
		if ctrl&v8WritingMask != 0 {
			goto retryBucket
		}
		words := v8LoadTagWords(b)
		match := v8MatchBits(words, tag)
		for match != 0 {
			lane := v8FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
				if status != v8OK {
					if status == v8Retry {
						goto retryBucket
					}
					return status, *new(V), false
				}
				var prev V
				if needValue {
					prev = e.val
				}
				if !v8EnableSameKeyTombstoneReuse {
					slot.WriteUnfenced(v8Entry[K, V]{})
				} else {
					slot.WriteUnfenced(v8Entry[K, V]{key: e.key})
				}
				v8StoreTag(b, lane, v8TagDeleted)
				v8EndWriteModified(b, ctrl)
				m.size.Add(v8CntTombstones, 1)
				return v8OK, prev, true
			}
			match &= match - 1
		}
		if v8EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v8OK, *new(V), false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v8Full, *new(V), false
}

func (m *V8Map[K, V]) compareAndSwapIn(
	table *v8Table[K, V],
	key *K,
	hash uintptr,
	old *V,
	new *V,
) (v8Status, bool) {
	tag, start := v8HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v8FrozenMask != 0 {
			return v8Frozen, false
		}
		if ctrl&v8WritingMask != 0 {
			goto retryBucket
		}
		words := v8LoadTagWords(b)
		match := v8MatchBits(words, tag)
		for match != 0 {
			lane := v8FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
					return v8OK, false
				}
				if m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(new))) {
					return v8OK, true
				}
				ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
				if status != v8OK {
					if status == v8Retry {
						goto retryBucket
					}
					return status, false
				}
				slot.WriteUnfenced(v8Entry[K, V]{key: e.key, val: *new})
				v8EndWriteModified(b, ctrl)
				return v8OK, true
			}
			match &= match - 1
		}
		if v8EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v8OK, false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v8Full, false
}

func (m *V8Map[K, V]) compareAndDeleteIn(
	table *v8Table[K, V],
	key *K,
	hash uintptr,
	old *V,
) (v8Status, bool) {
	tag, start := v8HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v8FrozenMask != 0 {
			return v8Frozen, false
		}
		if ctrl&v8WritingMask != 0 {
			goto retryBucket
		}
		words := v8LoadTagWords(b)
		match := v8MatchBits(words, tag)
		for match != 0 {
			lane := v8FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
					return v8OK, false
				}
				ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
				if status != v8OK {
					if status == v8Retry {
						goto retryBucket
					}
					return status, false
				}
				if !v8EnableSameKeyTombstoneReuse {
					slot.WriteUnfenced(v8Entry[K, V]{})
				} else {
					slot.WriteUnfenced(v8Entry[K, V]{key: e.key})
				}
				v8StoreTag(b, lane, v8TagDeleted)
				v8EndWriteModified(b, ctrl)
				m.size.Add(v8CntTombstones, 1)
				return v8OK, true
			}
			match &= match - 1
		}
		if v8EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v8OK, false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v8Full, false
}

func (m *V8Map[K, V]) computeIn(
	table *v8Table[K, V],
	key *K,
	hash uintptr,
	fn func(e *MapEntry[K, V]),
) (v8Status, V, bool, bool) {
	tag, start := v8HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v8FrozenMask != 0 {
			return v8Frozen, *new(V), false, false
		}
		if ctrl&v8WritingMask != 0 {
			goto retryBucket
		}
		words := v8LoadTagWords(b)
		match := v8MatchBits(words, tag)
		for match != 0 {
			lane := v8FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
				if status != v8OK {
					if status == v8Retry {
						goto retryBucket
					}
					return status, *new(V), false, false
				}
				it := MapEntry[K, V]{
					entry:  entry_[K, V]{hash: hash, key: *key, value: e.val},
					loaded: true,
				}
				fn(noEscape(&it))
				switch it.op {
				case updateOp:
					slot.WriteUnfenced(v8Entry[K, V]{key: e.key, val: it.entry.value})
					v8EndWriteModified(b, ctrl)
					return v8OK, it.entry.value, true, false
				case deleteOp:
					if !v8EnableSameKeyTombstoneReuse {
						slot.WriteUnfenced(v8Entry[K, V]{})
					} else {
						slot.WriteUnfenced(v8Entry[K, V]{key: e.key})
					}
					v8StoreTag(b, lane, v8TagDeleted)
					v8EndWriteModified(b, ctrl)
					m.size.Add(v8CntTombstones, 1)
					return v8OK, it.entry.value, true, false
				default:
					v8EndWriteUnchanged(b, ctrl)
					return v8OK, it.entry.value, true, false
				}
			}
			match &= match - 1
		}
		if v8EnableSameKeyTombstoneReuse {
			tombstones := v8DeletedBits(words)
			for tombstones != 0 {
				lane := v8FirstMarkedLane(tombstones)
				slot := table.entry(bi, lane)
				e := slot.ReadUnfenced()
				if ctrl != b.ctrl.Load() {
					goto retryBucket
				}
				if e.key == *key {
					ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
					if status != v8OK {
						if status == v8Retry {
							goto retryBucket
						}
						return status, *new(V), false, false
					}
					it := MapEntry[K, V]{
						entry: entry_[K, V]{hash: hash, key: *key},
					}
					fn(noEscape(&it))
					if it.op != updateOp {
						v8EndWriteUnchanged(b, ctrl)
						return v8OK, *new(V), false, false
					}
					slot.WriteUnfenced(v8Entry[K, V]{key: e.key, val: it.entry.value})
					v8StoreTag(b, lane, tag)
					v8EndWriteModified(b, ctrl)
					m.size.Add(v8CntTombstones, ^uintptr(0))
					return v8OK, it.entry.value, false, false
				}
				tombstones &= tombstones - 1
			}
		}
		if empty := v8EmptyBits(words); empty != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			ctrl, status := v8BeginWriteWithCtrl(b, ctrl)
			if status != v8OK {
				if status == v8Retry {
					goto retryBucket
				}
				return status, *new(V), false, false
			}
			lane := v8FirstMarkedLane(empty)
			it := MapEntry[K, V]{
				entry: entry_[K, V]{hash: hash, key: *key},
			}
			fn(noEscape(&it))
			if it.op != updateOp {
				v8EndWriteUnchanged(b, ctrl)
				return v8OK, *new(V), false, false
			}
			table.entry(bi, lane).WriteUnfenced(v8Entry[K, V]{key: *key, val: it.entry.value})
			v8StoreTag(b, lane, tag)
			v8EndWriteModified(b, ctrl)
			m.size.Add(v8CntOccupied, 1)
			return v8OK, it.entry.value, false, empty&(empty-1) == 0
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v8Full, *new(V), false, false
}

func (m *V8Map[K, V]) resizeIfNeeded(table *v8Table[K, V]) {
	occupied := int(m.size.Value(v8CntOccupied))
	if occupied >= table.growCap {
		m.tryResize(table, occupied, v8ResizeNormal)
		return
	}

	// Keep resize frequency low on write-heavy workloads. Tombstones remain
	// physical occupancy and are compacted when normal grow or probe-limit
	// resize happens; tryResize sizes the next table from live entries.
	//
	// The old eager compact threshold is intentionally disabled for now. It can
	// reduce miss-path tombstone drag, but it may trigger full-table copy long
	// before physical occupancy reaches growCap.
	//
	// tombstones := int(m.size.Value(v8CntTombstones))
	// live := occupied - tombstones
	// if tombstones > v8SlotsPerBucket && tombstones > live/4 {
	// 	m.tryResize(table, occupied, v8ResizeCompact)
	// }
}

func (m *V8Map[K, V]) tryResize(old *v8Table[K, V], occupied int, hint v8ResizeHint) *v8Table[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}
	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			// Base sizing follows the live entry count. At the normal grow
			// threshold this rounds to 2x; tombstone-heavy resize can stay at
			// the same size and compact tombstone slots away.
			tombstones := int(m.size.Value(v8CntTombstones))
			live := occupied - tombstones
			nextLen := v8CalcBucketLen(live)
			nextLen = max(nextLen, m.minLen)
			aggressive := hint == v8ResizeProbeLimit
			if v8EnableAggressiveGrow {
				curOccupied := int(m.size.Value(v8CntOccupied))
				occupiedInResize := curOccupied - occupied
				aggressive = aggressive || occupiedInResize >= 2
			}
			// Probe-limit resize or observed concurrent insert pressure gets
			// one extra size class, capped at 4x the old table.
			if aggressive {
				nextLen = min(nextLen<<1, old.bucketLen()<<2)
			}
			next = newV8Table[K, V](nextLen, old.intKey)
			old.nextTable.Store(next)
		} else {
			if v8EnableStoreInGrow {
				return old
			}
			for next == nil {
				runtime.Gosched()
				next = old.nextTable.Load()
			}
		}
	}
	return m.helpResizeInto(old, next)
}

func (m *V8Map[K, V]) helpResize(old *v8Table[K, V]) *v8Table[K, V] {
	next := old.nextTable.Load()
	if next == nil {
		return m.tryResize(old, int(m.size.Value(v8CntOccupied)), v8ResizeProbeLimit)
	}
	return m.helpResizeInto(old, next)
}

func (m *V8Map[K, V]) helpResizeInto(old, next *v8Table[K, V]) *v8Table[K, V] {
	// Cooperative resize.
	// Each slot is frozen before it is copied. This removes the old global
	// freeze barrier while preserving the key rule: a copied slot can no longer
	// be modified in the old table.
	for {
		chunk := old.copyIdx.Add(1) - 1
		if chunk >= old.chunks {
			for {
				table := m.table.Load()
				if table != old {
					return table
				}
				runtime.Gosched()
			}
		}
		copyMaxProbe := uintptr(0)
		start := uintptr(chunk) * old.chunkSz
		end := min(start+old.chunkSz, old.bucketLen())
		for i := start; i < end; i++ {
			b := old.buckets.At(i)
			words := v8FreezeAndLoadTags(b)
			full := v8FullBits(words)
			for full != 0 {
				lane := v8FirstMarkedLane(full)
				e := old.entry(i, lane).ReadUnfenced()
				probe := next.copyInsertConcurrent(e, m.hashKey(noEscape(&e.key)))
				if probe > copyMaxProbe {
					copyMaxProbe = probe
				}
				full &= full - 1
			}
		}
		for {
			cur := next.copyMaxProbe.Load()
			if copyMaxProbe <= cur {
				break
			}
			if next.copyMaxProbe.CompareAndSwap(cur, copyMaxProbe) {
				break
			}
		}
		if old.copyDone.Add(1) == old.chunks {
			// Adaptive probe limit: tighten based on the max probe distance
			// actually observed during migration. This allows the window to
			// shrink when clustering dissipates after table growth or
			// tombstone compaction, avoiding a permanently inflated miss path.
			observed := next.copyMaxProbe.Load() + 1
			nextLen := next.bucketLen()
			next.probeLimit = min(nextLen, max(observed<<1, calcProbeLimit(nextLen)))
			occupied := m.size.Reset(v8CntOccupied)
			tombstones := m.size.Reset(v8CntTombstones)
			m.size.Add(v8CntOccupied, occupied-tombstones)
			m.table.CompareAndSwap(old, next)
		}
	}
}

func (m *V8Map[K, V]) ensureTable() *v8Table[K, V] {
	if table := m.table.Load(); table != nil {
		return table
	}
	return m.slowInit()
}

//go:noinline
func (m *V8Map[K, V]) slowInit() *v8Table[K, V] {
	for {
		table := m.table.Load()
		if table != nil {
			return table
		}
		if m.initState.CompareAndSwap(0, 1) {
			table = m.table.Load()
			if table == nil {
				var cfg MapConfig
				m.init(noEscape(&cfg))
				table = m.table.Load()
			}
			return table
		}
		runtime.Gosched()
	}
}

//go:nosplit
func (m *V8Map[K, V]) hashKey(key *K) uintptr {
	if m.intKey {
		return intHash[K](noescape(unsafe.Pointer(key)))
	}
	return m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
}

func newV8Table[K comparable, V any](bucketLen uintptr, intKey bool) *v8Table[K, V] {
	bucketLen = nextPowOf2(max(bucketLen, uintptr(v8MinBuckets)))
	slotLen := bucketLen * v8SlotsPerBucket
	growCap := int(slotLen * v8LoadFactorNum / v8LoadFactorDen)
	cpus := max(uintptr(runtime.GOMAXPROCS(0)), 1)
	roundedSizeLen := nextPowOf2(cpus)
	stripeCap := int(uintptr(growCap) >> bits.TrailingZeros(uint(roundedSizeLen)))
	chunks, chunkSz := v8ResizeChunks(bucketLen, cpus)
	buckets, bucketBacking := makeV8Buckets(bucketLen)
	entries := makeUnsafeSlice[SeqLockSlot[v8Entry[K, V]]](slotLen)
	return &v8Table[K, V]{
		buckets:       buckets,
		entries:       entries,
		mask:          bucketLen - 1,
		probeLimit:    min(bucketLen, calcProbeLimit(bucketLen)),
		stripeCap:     stripeCap,
		growCap:       growCap,
		intKey:        intKey,
		chunks:        chunks,
		chunkSz:       chunkSz,
		bucketBacking: bucketBacking,
	}
}

func makeV8Buckets(bucketLen uintptr) (unsafeSlice[v8Bucket], unsafe.Pointer) {
	stride := unsafe.Sizeof(v8Bucket{})
	align := stride
	backing := make([]byte, bucketLen*stride+align-1)
	basePtr := unsafe.Pointer(unsafe.SliceData(backing))
	base := uintptr(basePtr)
	aligned := (base + align - 1) &^ (align - 1)
	return unsafeSlice[v8Bucket]{ptr: unsafe.Pointer(aligned)}, basePtr //nolint:all
}

//go:nosplit
func v8HashParts(hash uintptr, intKey bool, mask uintptr) (uint8, uintptr) {
	// V4 uses h2-format tags: the high bit marks a full lane and the lower
	// seven bits carry entropy. The lost tag bit buys a cheaper SWAR full-lane
	// test: v8FullBits can be just a high-bit mask.
	if intKey {
		mixed := uint64(hash) * uint64(0x9e3779b97f4a7c15)
		tag := h2(uintptr(mixed >> 56))
		return tag, uintptr(mixed>>32) & mask
	}
	return h2(hash), h1(hash) & mask
}

//go:nosplit
func v8CalcBucketLen(capacity int) uintptr {
	if capacity <= 0 {
		return v8MinBuckets
	}
	needSlots := uintptr(capacity+1) * v8LoadFactorDen / v8LoadFactorNum
	needBuckets := (needSlots + v8SlotsPerBucket - 1) / v8SlotsPerBucket
	return nextPowOf2(max(needBuckets, uintptr(v8MinBuckets)))
}

//go:nosplit
func v8ResizeChunks(bucketLen, cpus uintptr) (chunks uint32, chunkSz uintptr) {
	const overCpus = resizeOverPartition
	want := min(bucketLen/v8MinBuckets, max(cpus*overCpus, 1))
	if want <= 1 {
		return 1, bucketLen
	}
	c := uint32(1) << (bits.Len32(uint32(want)) - 1)
	return c, bucketLen >> bits.TrailingZeros32(c)
}

//go:nosplit
func (table *v8Table[K, V]) bucketLen() uintptr {
	return table.mask + 1
}

//go:nosplit
func (table *v8Table[K, V]) entry(bucketIdx, lane uintptr) *SeqLockSlot[v8Entry[K, V]] {
	return table.entries.At(bucketIdx*v8SlotsPerBucket + lane)
}

func (table *v8Table[K, V]) copyInsertConcurrent(e v8Entry[K, V], hash uintptr) uintptr {
	tag, start := v8HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
		ctrl, status := v8BeginWrite(b)
		if status != v8OK {
			probe--
			continue
		}
		words := v8LoadTagWords(b)
		empty := v8EmptyBits(words)
		if empty == 0 {
			v8EndWriteUnchanged(b, ctrl)
			continue
		}
		lane := v8FirstMarkedLane(empty)
		dst := table.entry(bi, lane)
		dst.WriteUnfenced(e)
		v8StoreTag(b, lane, tag)
		v8EndWriteModified(b, ctrl)
		return probe
	}
	panic("cc: V8Map grow produced a full table")
}

func v8BeginWrite(b *v8Bucket) (uint32, v8Status) {
	ctrl := b.ctrl.Load()
	return v8BeginWriteWithCtrl(b, ctrl)
}

func v8BeginWriteWithCtrl(b *v8Bucket, ctrl uint32) (uint32, v8Status) {
	if ctrl&v8FrozenMask != 0 {
		return 0, v8Frozen
	}
	if ctrl&v8WritingMask != 0 {
		return 0, v8Retry
	}
	if !b.ctrl.CompareAndSwap(ctrl, ctrl|v8WritingMask) {
		return 0, v8Retry
	}
	return ctrl, v8OK
}

func v8EndWriteUnchanged(b *v8Bucket, ctrl uint32) {
	b.ctrl.Store(ctrl)
}

func v8EndWriteModified(b *v8Bucket, ctrl uint32) {
	b.ctrl.Store(v8BumpCtrl(ctrl))
}

func v8FreezeAndLoadTags(b *v8Bucket) uint64 {
	for {
		ctrl := b.ctrl.Load()
		if ctrl&v8FrozenMask != 0 {
			return v8LoadTagWords(b)
		}
		if ctrl&v8WritingMask != 0 {
			continue
		}
		if b.ctrl.CompareAndSwap(ctrl, ctrl|v8WritingMask) {
			b.ctrl.Store(v8BumpCtrl(ctrl | v8FrozenMask))
			return v8LoadTagWords(b)
		}
	}
}

// v8BumpCtrl increments the version portion of the control word.
// Note: We do not need to clear v8WritingMask here because the returned
// ctrl from beginWrite does not have the writing mask set, and we just
// add v8VersionInc to it.
//
//go:nosplit
func v8BumpCtrl(ctrl uint32) uint32 {
	return ctrl + v8VersionInc
}

//go:nosplit
func v8LoadTagWords(b *v8Bucket) uint64 {
	return b.tags.Load()
}

//go:nosplit
func v8StoreTag(b *v8Bucket, lane uintptr, tag uint8) {
	shift := (lane & (v8SlotsPerBucket - 1)) << 3
	mask := uint64(0xff) << shift
	b.tags.Store((b.tags.Load() &^ mask) | uint64(tag)<<shift)
}

//go:nosplit
func v8MatchBits(words uint64, tag uint8) uint64 {
	// The match path verifies candidate lanes with the full key after taking a
	// ctrl snapshot, so the fast SWAR zero check may return harmless false
	// positives. Empty/deleted checks below need the exact variant because they
	// control probe termination and tombstone reuse.
	return v8MaybeZeroByteBits(words ^ v8BroadcastTag(tag))
}

//go:nosplit
func v8EmptyBits(words uint64) uint64 {
	return v8ZeroByteBits(words)
}

//go:nosplit
func v8DeletedBits(words uint64) uint64 {
	return v8ZeroByteBits(words ^ v8BroadcastTag(v8TagDeleted))
}

//go:nosplit
func v8FullBits(words uint64) uint64 {
	// Full tags use the h2 byte format and always carry the high bit. Empty (0)
	// and deleted (1) never do, so a high-bit mask is enough here.
	return words & v8LaneMarkerMask
}

//go:nosplit
func v8BroadcastTag(tag uint8) uint64 {
	return uint64(tag) * 0x0101010101010101
}

//go:nosplit
func v8MaybeZeroByteBits(words uint64) uint64 {
	const lowBits = uint64(0x0101010101010101)
	return (words - lowBits) &^ words & v8LaneMarkerMask
}

//go:nosplit
func v8ZeroByteBits(words uint64) uint64 {
	const lowBits = uint64(0x7f7f7f7f7f7f7f7f)
	return ^(((words & lowBits) + lowBits) | words | lowBits) & v8LaneMarkerMask
}

//go:nosplit
func v8FirstMarkedLane(marked uint64) uintptr {
	return uintptr(bits.TrailingZeros64(marked)) >> 3
}
