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
	// v4EnableIntKey enables the specialized integer hash/start path.
	v4EnableIntKey = true
	// v4EnableDedupVal skips publishing an update when the new value equals
	// the existing value and a value equality function is available.
	v4EnableDedupVal = true
	// v4EnableStoreInGrow lets writers keep retrying against the old table
	// while a resize leader is allocating/publishing the next table. Keep it
	// off by default so writers join cooperative resize instead of extending
	// contention on old buckets.
	v4EnableStoreInGrow = false
	// v4EnableAggressiveGrow adds one extra size class on probe-limit resize
	// or observed concurrent insert pressure during table allocation.
	v4EnableAggressiveGrow = true
	// v4EnableSameKeyTombstoneReuse lets a Store revive a tombstone left by
	// the same key. When disabled, deletes clear both key and value while the
	// tombstone remains as a probe-continuation marker until resize compaction.
	v4EnableSameKeyTombstoneReuse = true
)

const (
	v4MinBuckets     = 32
	v4SlotsPerBucket = 4
	v4LoadFactor     = 0.75
	v4LaneMarkerMask = uint32(0x80808080)
)

const (
	v4TagEmpty   = uint8(0)
	v4TagDeleted = uint8(1)
)

const (
	// Keep ctrl for version/frozen/writing only. If miss-heavy workloads need a
	// probe-continuation hint, prefer a side structure so displaced inserts do not
	// have to update shared bucket metadata.
	v4WritingMask = uint32(1) << 0
	v4FrozenMask  = uint32(1) << 1
	v4VersionMask = uint32(0xFFFFFFFC)
	v4VersionInc  = uint32(1) << 2
)

const (
	// occupied tracks physical lanes that are no longer Empty: Full + Tombstone.
	// tombstones tracks lanes currently in Deleted/Tombstone state, not total
	// delete operations.
	v4CntOccupied = iota
	v4CntTombstones
)

type v4Status uint8

const (
	v4OK v4Status = iota
	v4Full
	v4Retry
	v4Frozen
)

type v4ResizeHint uint8

const (
	v4ResizeNormal v4ResizeHint = iota
	v4ResizeProbeLimit
	v4ResizeCompact
)

// V4Map is an experimental SWAR-probed open-addressed map.
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
type V4Map[K comparable, V any] struct {
	_         noCopy
	table     atomic.Pointer[v4Table[K, V]]
	initState atomic.Uint32
	intKey    bool
	seed      uintptr
	keyHash   HashFunc
	valEqual  EqualFunc
	minLen    uintptr
}

type v4Table[K comparable, V any] struct {
	buckets      unsafeSlice[v4Bucket]
	entries      unsafeSlice[SeqLockSlot[v4Entry[K, V]]]
	mask         uintptr
	probeLimit   uintptr
	stripeCap    int
	growCap      int
	size         FixedLocalCounterN
	chunkSz      uintptr
	chunks       uint32
	allocating   atomic.Uint32
	copyIdx      atomic.Uint32
	copyDone     atomic.Uint32
	copyMaxProbe atomic.Uintptr
	nextTable    atomic.Pointer[v4Table[K, V]]
}

// v4Bucket stores the tags and control word for a bucket.
// Layout: 4 bytes for 4 slots of tags, 4 bytes for control metadata.
// Total 8 bytes.
// Alignment: 4-byte aligned (due to atomic.Uint32).
// Padding: Perfectly packed, 0 bytes of padding.
//
// ┌───────────────────────────────────────────────────────────────────┐
// │                    4 × 8-bit h2 tags (32 bits)                    │
// │                             bytes 0-3                             │
// ├──────────────────────────────────────────────┬──────┬─────────────┤
// │              version (30 bits)               │frozen│   writing   │
// │                  bits 31-2                   │bit 1 │    bit 0    │
// └──────────────────────────────────────────────┴──────┴─────────────┘
type v4Bucket struct {
	tags atomic.Uint32 // [4] bytes of tag
	ctrl atomic.Uint32
}

type v4Entry[K comparable, V any] struct {
	key K
	val V
}

func NewV4Map[K comparable, V any](options ...func(*MapConfig)) *V4Map[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m := &V4Map[K, V]{}
	m.init(noEscape(&cfg))
	return m
}

func (m *V4Map[K, V]) init(cfg *MapConfig) {
	if cfg.keyHash == nil {
		cfg.keyHash = parseKeyInterface[K]()
	}
	if cfg.valEqual == nil {
		cfg.valEqual = parseValueInterface[V]()
	}
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	m.intKey = v4EnableIntKey && m.intKey
	if cfg.keyHash != nil {
		m.keyHash = cfg.keyHash
		m.intKey = false
	}
	if cfg.valEqual != nil {
		m.valEqual = cfg.valEqual
	}
	m.seed = uintptr(rand.Uint64())
	m.minLen = v4CalcBucketLen(cfg.capacity)
	m.table.Store(newV4Table[K, V](m.minLen))
}

func (m *V4Map[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	hash := m.hashKey(noEscape(&key))
	tag, start := v4HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
		// spins := 0
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v4WritingMask != 0 {
			// delay(&spins)
			goto retryBucket
		}
		words := v4LoadTagWords(b)
		match := v4MatchBits(words, tag)
		for match != 0 {
			lane := v4FirstMarkedLane(match)
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
		if v4EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return *new(V), false
		}
	}
	return *new(V), false
}

func (m *V4Map[K, V]) Store(key K, value V) {
	m.store(noEscape(&key), noEscape(&value), false)
}

func (m *V4Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.store(noEscape(&key), noEscape(&value), true)
}

func (m *V4Map[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	return m.update(noEscape(&key), noEscape(&value), false)
}

func (m *V4Map[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.delete(noEscape(&key), true)
}

func (m *V4Map[K, V]) Delete(key K) {
	m.delete(noEscape(&key), false)
}

func (m *V4Map[K, V]) CompareAndSwap(key K, old V, new V) bool {
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
		case v4OK:
			return swapped
		case v4Frozen:
			table = m.helpResize(table)
		case v4Full:
			return false
		}
	}
}

func (m *V4Map[K, V]) CompareAndDelete(key K, old V) bool {
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
		case v4OK:
			return deleted
		case v4Frozen:
			table = m.helpResize(table)
		case v4Full:
			return false
		}
	}
}

func (m *V4Map[K, V]) Compute(key K, fn func(e *MapEntry[K, V])) (actual V, loaded bool) {
	table := m.ensureTable()
	hash := m.hashKey(noEscape(&key))
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.computeIn(table, noEscape(&key), hash, fn)
		switch status {
		case v4OK:
			if !loaded && shouldCheckResize && int(table.size.Get(v4CntOccupied)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v4Full:
			table = m.tryResize(table, int(table.size.Value(v4CntOccupied)), v4ResizeProbeLimit)
		case v4Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V4Map[K, V]) Range(yield func(K, V) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}

	var cache [v4SlotsPerBucket]v4Entry[K, V]
	unsafeCache := toUnsafeSlice(&cache[0])
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
	retry:
		ctrl := b.ctrl.Load()
		if ctrl&v4WritingMask != 0 {
			goto retry
		}
		words := v4LoadTagWords(b)
		full := v4FullBits(words)
		var cacheCount uintptr
		for full != 0 {
			lane := v4FirstMarkedLane(full)
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

func (m *V4Map[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

type v4MapStats struct {
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

func (m *V4Map[K, V]) stats() v4MapStats {
	table := m.table.Load()
	var occupied, tombstones uintptr
	if table != nil {
		occupied = table.size.Value(v4CntOccupied)
		tombstones = table.size.Value(v4CntTombstones)
	}
	stats := v4MapStats{
		Live:       occupied - tombstones,
		Occupied:   occupied,
		Tombstones: tombstones,
	}
	if table == nil {
		return stats
	}
	stats.Buckets = table.bucketLen()
	stats.Capacity = stats.Buckets * v4SlotsPerBucket
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
		words := v4LoadTagWords(b)
		full := v4FullBits(words)
		empty := v4EmptyBits(words)
		tombstones := v4DeletedBits(words)
		fullCount := uintptr(bits.OnesCount32(full))
		stats.FullLanes += fullCount
		stats.EmptyLanes += uintptr(bits.OnesCount32(empty))
		stats.TombstoneLanes += uintptr(bits.OnesCount32(tombstones))
		if fullCount == v4SlotsPerBucket {
			stats.FullBuckets++
		}
		fullScan := full
		for fullScan != 0 {
			lane := v4FirstMarkedLane(fullScan)
			e := table.entry(i, lane).ReadUnfenced()
			hash := m.hashKey(noEscape(&e.key))
			_, start := v4HashParts(hash, m.intKey, table.mask)
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

func (m *V4Map[K, V]) Size() int {
	table := m.table.Load()
	if table == nil {
		return 0
	}
	occupied := int(table.size.Value(v4CntOccupied))
	tombstones := int(table.size.Value(v4CntTombstones))
	return max(occupied-tombstones, 0)
}

func (m *V4Map[K, V]) Clear() {
	if m.table.Load() == nil {
		return
	}
	m.table.Store(newV4Table[K, V](m.minLen))
}

func (m *V4Map[K, V]) store(key *K, val *V, onlyIfAbsent bool) (actual V, loaded bool) {
	table := m.ensureTable()
	hash := m.hashKey(key)
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.storeIn(table, key, val, hash, onlyIfAbsent)
		switch status {
		case v4OK:
			if !loaded && shouldCheckResize && int(table.size.Get(v4CntOccupied)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v4Full:
			table = m.tryResize(table, int(table.size.Value(v4CntOccupied)), v4ResizeProbeLimit)
		case v4Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V4Map[K, V]) update(key *K, val *V, onlyIfAbsent bool) (previous V, loaded bool) {
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
		status, previous, loaded := m.updateIn(table, key, val, hash, onlyIfAbsent)
		switch status {
		case v4OK:
			return previous, loaded
		case v4Full:
			return *new(V), false
		case v4Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V4Map[K, V]) storeIn(
	table *v4Table[K, V],
	key *K,
	val *V,
	hash uintptr,
	onlyIfAbsent bool,
) (v4Status, V, bool, bool) {
	tag, start := v4HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v4FrozenMask != 0 {
			return v4Frozen, *new(V), false, false
		}
		if ctrl&v4WritingMask != 0 {
			goto retryBucket
		}
		words := v4LoadTagWords(b)
		match := v4MatchBits(words, tag)
		for match != 0 {
			lane := v4FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if onlyIfAbsent {
					return v4OK, e.val, true, false
				}
				if v4EnableDedupVal && m.valEqual != nil &&
					m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return v4OK, e.val, true, false
				}
				ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
				if status != v4OK {
					if status == v4Retry {
						goto retryBucket
					}
					return status, *new(V), false, false
				}
				slot.WriteUnfenced(v4Entry[K, V]{key: e.key, val: *val})
				v4EndWriteModified(b, ctrl)
				return v4OK, *val, true, false
			}
			match &= match - 1
		}
		if v4EnableSameKeyTombstoneReuse {
			tombstones := v4DeletedBits(words)
			for tombstones != 0 {
				lane := v4FirstMarkedLane(tombstones)
				slot := table.entry(bi, lane)
				e := slot.ReadUnfenced()
				if ctrl != b.ctrl.Load() {
					goto retryBucket
				}
				if e.key == *key {
					ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
					if status != v4OK {
						if status == v4Retry {
							goto retryBucket
						}
						return status, *new(V), false, false
					}
					slot.WriteUnfenced(v4Entry[K, V]{key: e.key, val: *val})
					v4StoreTag(b, lane, tag)
					v4EndWriteModified(b, ctrl)
					table.size.Add(v4CntTombstones, ^uintptr(0))
					return v4OK, *val, false, false
				}
				tombstones &= tombstones - 1
			}
		}
		if empty := v4EmptyBits(words); empty != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
			if status != v4OK {
				if status == v4Retry {
					goto retryBucket
				}
				return status, *new(V), false, false
			}
			lane := v4FirstMarkedLane(empty)
			table.entry(bi, lane).WriteUnfenced(v4Entry[K, V]{key: *key, val: *val})
			v4StoreTag(b, lane, tag)
			v4EndWriteModified(b, ctrl)
			table.size.Add(v4CntOccupied, 1)
			return v4OK, *val, false, empty&(empty-1) == 0
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v4Full, *new(V), false, false
}

func (m *V4Map[K, V]) updateIn(
	table *v4Table[K, V],
	key *K,
	val *V,
	hash uintptr,
	onlyIfAbsent bool,
) (v4Status, V, bool) {
	tag, start := v4HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v4FrozenMask != 0 {
			return v4Frozen, *new(V), false
		}
		if ctrl&v4WritingMask != 0 {
			goto retryBucket
		}
		words := v4LoadTagWords(b)
		match := v4MatchBits(words, tag)
		for match != 0 {
			lane := v4FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if onlyIfAbsent {
					return v4OK, e.val, true
				}
				if v4EnableDedupVal && m.valEqual != nil &&
					m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return v4OK, e.val, true
				}
				ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
				if status != v4OK {
					if status == v4Retry {
						goto retryBucket
					}
					return status, *new(V), false
				}
				slot.WriteUnfenced(v4Entry[K, V]{key: e.key, val: *val})
				v4EndWriteModified(b, ctrl)
				return v4OK, e.val, true
			}
			match &= match - 1
		}
		if v4EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v4OK, *new(V), false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v4OK, *new(V), false
}

func (m *V4Map[K, V]) delete(key *K, needValue bool) (previous V, loaded bool) {
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
		case v4OK:
			return previous, loaded
		case v4Full:
			return *new(V), false
		case v4Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V4Map[K, V]) deleteIn(
	table *v4Table[K, V],
	key *K,
	hash uintptr,
	needValue bool,
) (v4Status, V, bool) {
	tag, start := v4HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v4FrozenMask != 0 {
			return v4Frozen, *new(V), false
		}
		if ctrl&v4WritingMask != 0 {
			goto retryBucket
		}
		words := v4LoadTagWords(b)
		match := v4MatchBits(words, tag)
		for match != 0 {
			lane := v4FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
				if status != v4OK {
					if status == v4Retry {
						goto retryBucket
					}
					return status, *new(V), false
				}
				var prev V
				if needValue {
					prev = e.val
				}
				if !v4EnableSameKeyTombstoneReuse {
					slot.WriteUnfenced(v4Entry[K, V]{})
				} else {
					slot.WriteUnfenced(v4Entry[K, V]{key: e.key})
				}
				v4StoreTag(b, lane, v4TagDeleted)
				v4EndWriteModified(b, ctrl)
				table.size.Add(v4CntTombstones, 1)
				return v4OK, prev, true
			}
			match &= match - 1
		}
		if v4EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v4OK, *new(V), false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v4Full, *new(V), false
}

func (m *V4Map[K, V]) compareAndSwapIn(
	table *v4Table[K, V],
	key *K,
	hash uintptr,
	old *V,
	new *V,
) (v4Status, bool) {
	tag, start := v4HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v4FrozenMask != 0 {
			return v4Frozen, false
		}
		if ctrl&v4WritingMask != 0 {
			goto retryBucket
		}
		words := v4LoadTagWords(b)
		match := v4MatchBits(words, tag)
		for match != 0 {
			lane := v4FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
					return v4OK, false
				}
				if m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(new))) {
					return v4OK, true
				}
				ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
				if status != v4OK {
					if status == v4Retry {
						goto retryBucket
					}
					return status, false
				}
				slot.WriteUnfenced(v4Entry[K, V]{key: e.key, val: *new})
				v4EndWriteModified(b, ctrl)
				return v4OK, true
			}
			match &= match - 1
		}
		if v4EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v4OK, false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v4Full, false
}

func (m *V4Map[K, V]) compareAndDeleteIn(
	table *v4Table[K, V],
	key *K,
	hash uintptr,
	old *V,
) (v4Status, bool) {
	tag, start := v4HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v4FrozenMask != 0 {
			return v4Frozen, false
		}
		if ctrl&v4WritingMask != 0 {
			goto retryBucket
		}
		words := v4LoadTagWords(b)
		match := v4MatchBits(words, tag)
		for match != 0 {
			lane := v4FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
					return v4OK, false
				}
				ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
				if status != v4OK {
					if status == v4Retry {
						goto retryBucket
					}
					return status, false
				}
				if !v4EnableSameKeyTombstoneReuse {
					slot.WriteUnfenced(v4Entry[K, V]{})
				} else {
					slot.WriteUnfenced(v4Entry[K, V]{key: e.key})
				}
				v4StoreTag(b, lane, v4TagDeleted)
				v4EndWriteModified(b, ctrl)
				table.size.Add(v4CntTombstones, 1)
				return v4OK, true
			}
			match &= match - 1
		}
		if v4EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v4OK, false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v4Full, false
}

func (m *V4Map[K, V]) computeIn(
	table *v4Table[K, V],
	key *K,
	hash uintptr,
	fn func(e *MapEntry[K, V]),
) (v4Status, V, bool, bool) {
	tag, start := v4HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v4FrozenMask != 0 {
			return v4Frozen, *new(V), false, false
		}
		if ctrl&v4WritingMask != 0 {
			goto retryBucket
		}
		words := v4LoadTagWords(b)
		match := v4MatchBits(words, tag)
		for match != 0 {
			lane := v4FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if e.key == *key {
				ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
				if status != v4OK {
					if status == v4Retry {
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
					slot.WriteUnfenced(v4Entry[K, V]{key: e.key, val: it.entry.value})
					v4EndWriteModified(b, ctrl)
					return v4OK, it.entry.value, true, false
				case deleteOp:
					if !v4EnableSameKeyTombstoneReuse {
						slot.WriteUnfenced(v4Entry[K, V]{})
					} else {
						slot.WriteUnfenced(v4Entry[K, V]{key: e.key})
					}
					v4StoreTag(b, lane, v4TagDeleted)
					v4EndWriteModified(b, ctrl)
					table.size.Add(v4CntTombstones, 1)
					return v4OK, it.entry.value, true, false
				default:
					v4EndWriteUnchanged(b, ctrl)
					return v4OK, it.entry.value, true, false
				}
			}
			match &= match - 1
		}
		if v4EnableSameKeyTombstoneReuse {
			tombstones := v4DeletedBits(words)
			for tombstones != 0 {
				lane := v4FirstMarkedLane(tombstones)
				slot := table.entry(bi, lane)
				e := slot.ReadUnfenced()
				if ctrl != b.ctrl.Load() {
					goto retryBucket
				}
				if e.key == *key {
					ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
					if status != v4OK {
						if status == v4Retry {
							goto retryBucket
						}
						return status, *new(V), false, false
					}
					it := MapEntry[K, V]{
						entry: entry_[K, V]{hash: hash, key: *key},
					}
					fn(noEscape(&it))
					if it.op != updateOp {
						v4EndWriteUnchanged(b, ctrl)
						return v4OK, *new(V), false, false
					}
					slot.WriteUnfenced(v4Entry[K, V]{key: e.key, val: it.entry.value})
					v4StoreTag(b, lane, tag)
					v4EndWriteModified(b, ctrl)
					table.size.Add(v4CntTombstones, ^uintptr(0))
					return v4OK, it.entry.value, false, false
				}
				tombstones &= tombstones - 1
			}
		}
		if empty := v4EmptyBits(words); empty != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			ctrl, status := v4BeginWriteWithCtrl(b, ctrl)
			if status != v4OK {
				if status == v4Retry {
					goto retryBucket
				}
				return status, *new(V), false, false
			}
			lane := v4FirstMarkedLane(empty)
			it := MapEntry[K, V]{
				entry: entry_[K, V]{hash: hash, key: *key},
			}
			fn(noEscape(&it))
			if it.op != updateOp {
				v4EndWriteUnchanged(b, ctrl)
				return v4OK, *new(V), false, false
			}
			table.entry(bi, lane).WriteUnfenced(v4Entry[K, V]{key: *key, val: it.entry.value})
			v4StoreTag(b, lane, tag)
			v4EndWriteModified(b, ctrl)
			table.size.Add(v4CntOccupied, 1)
			return v4OK, it.entry.value, false, empty&(empty-1) == 0
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v4Full, *new(V), false, false
}

func (m *V4Map[K, V]) resizeIfNeeded(table *v4Table[K, V]) {
	if v4EnableStoreInGrow {
		if table.allocating.Load() != 0 {
			return
		}
	}
	occupied := int(table.size.Value(v4CntOccupied))
	if occupied >= table.growCap {
		m.tryResize(table, occupied, v4ResizeNormal)
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
	// tombstones := int(table.size.Value(v4CntTombstones))
	// live := occupied - tombstones
	// if tombstones > v4SlotsPerBucket && tombstones > live/4 {
	// 	m.tryResize(table, occupied, v4ResizeCompact)
	// }
}

func (m *V4Map[K, V]) tryResize(old *v4Table[K, V], occupied int, hint v4ResizeHint) *v4Table[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}
	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			// Base sizing follows the live entry count. At the normal grow
			// threshold this rounds to 2x; tombstone-heavy resize can stay at
			// the same size and compact tombstone slots away.
			tombstones := int(old.size.Value(v4CntTombstones))
			live := occupied - tombstones
			nextLen := v4CalcBucketLen(live)
			nextLen = max(nextLen, m.minLen)
			aggressive := hint == v4ResizeProbeLimit
			if v4EnableAggressiveGrow {
				curOccupied := int(old.size.Value(v4CntOccupied))
				occupiedInResize := curOccupied - occupied
				aggressive = aggressive || occupiedInResize >= 2
			}
			// Probe-limit resize or observed concurrent insert pressure gets
			// one extra size class, capped at 4x the old table.
			if aggressive {
				nextLen = min(nextLen<<1, old.bucketLen()<<2)
			}
			next = newV4Table[K, V](nextLen)
			old.nextTable.Store(next)
		} else {
			if v4EnableStoreInGrow {
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

func (m *V4Map[K, V]) helpResize(old *v4Table[K, V]) *v4Table[K, V] {
	next := old.nextTable.Load()
	if next == nil {
		return m.tryResize(old, int(old.size.Value(v4CntOccupied)), v4ResizeProbeLimit)
	}
	return m.helpResizeInto(old, next)
}

func (m *V4Map[K, V]) helpResizeInto(old, next *v4Table[K, V]) *v4Table[K, V] {
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
		copied := uintptr(0)
		for i := start; i < end; i++ {
			b := old.buckets.At(i)
			words := v4FreezeAndLoadTags(b)
			full := v4FullBits(words)
			for full != 0 {
				lane := v4FirstMarkedLane(full)
				e := old.entry(i, lane).ReadUnfenced()
				probe := next.copyInsertConcurrent(e, m.hashKey(noEscape(&e.key)), m.intKey)
				copied++
				if probe > copyMaxProbe {
					copyMaxProbe = probe
				}
				full &= full - 1
			}
		}
		if copied != 0 {
			next.size.Add(v4CntOccupied, copied)
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
			m.table.CompareAndSwap(old, next)
		}
	}
}

func (m *V4Map[K, V]) ensureTable() *v4Table[K, V] {
	if table := m.table.Load(); table != nil {
		return table
	}
	return m.slowInit()
}

//go:noinline
func (m *V4Map[K, V]) slowInit() *v4Table[K, V] {
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
func (m *V4Map[K, V]) hashKey(key *K) uintptr {
	if m.intKey {
		return intHash[K](noescape(unsafe.Pointer(key)))
	}
	return m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
}

func newV4Table[K comparable, V any](bucketLen uintptr) *v4Table[K, V] {
	bucketLen = nextPowOf2(max(bucketLen, uintptr(v4MinBuckets)))
	slotLen := bucketLen * v4SlotsPerBucket
	growCap := int(float64(slotLen) * v4LoadFactor)
	cpus := maxProcs()
	sizeLen := calcSizeLen(bucketLen, cpus)
	chunks, chunkSz := v4ResizeChunks(bucketLen, cpus)
	buckets := makeUnsafeSlice[v4Bucket](bucketLen)
	entries := makeUnsafeSlice[SeqLockSlot[v4Entry[K, V]]](slotLen)
	table := &v4Table[K, V]{
		buckets:    buckets,
		entries:    entries,
		mask:       bucketLen - 1,
		probeLimit: min(bucketLen, calcProbeLimit(bucketLen)),
		stripeCap:  max(growCap/int(sizeLen), 1),
		growCap:    growCap,
		size:       NewFixedLocalCounterN(sizeLen),
		chunks:     chunks,
		chunkSz:    chunkSz,
	}
	return table
}

//go:nosplit
func v4HashParts(hash uintptr, intKey bool, mask uintptr) (uint8, uintptr) {
	// V4 uses h2-format tags: the high bit marks a full lane and the lower
	// seven bits carry entropy. The lost tag bit buys a cheaper SWAR full-lane
	// test: v4FullBits can be just a high-bit mask.
	if intKey {
		mixed := uint64(hash) * uint64(0x9e3779b97f4a7c15)
		tag := h2(uintptr(mixed >> 56))
		return tag, uintptr(mixed>>32) & mask
	}
	return h2(hash), h1(hash) & mask
}

//go:nosplit
func v4CalcBucketLen(capacity int) uintptr {
	if capacity <= 0 {
		return v4MinBuckets
	}
	const invLoadFactor = 1 / v4LoadFactor
	needSlots := uintptr(float64(capacity+1) * invLoadFactor)
	needBuckets := (needSlots + v4SlotsPerBucket - 1) / v4SlotsPerBucket
	return nextPowOf2(max(needBuckets, uintptr(v4MinBuckets)))
}

//go:nosplit
func v4ResizeChunks(bucketLen, cpus uintptr) (chunks uint32, chunkSz uintptr) {
	const overCpus = resizeOverPartition
	want := min(bucketLen/v4MinBuckets, max(cpus*overCpus, 1))
	if want <= 1 {
		return 1, bucketLen
	}
	c := uint32(1) << (bits.Len32(uint32(want)) - 1)
	return c, bucketLen >> bits.TrailingZeros32(c)
}

//go:nosplit
func (table *v4Table[K, V]) bucketLen() uintptr {
	return table.mask + 1
}

//go:nosplit
func (table *v4Table[K, V]) entry(bucketIdx, lane uintptr) *SeqLockSlot[v4Entry[K, V]] {
	return table.entries.At(bucketIdx*v4SlotsPerBucket + lane)
}

func (table *v4Table[K, V]) copyInsertConcurrent(e v4Entry[K, V], hash uintptr, intKey bool) uintptr {
	tag, start := v4HashParts(hash, intKey, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
		ctrl, status := v4BeginWrite(b)
		if status != v4OK {
			probe--
			continue
		}
		words := v4LoadTagWords(b)
		empty := v4EmptyBits(words)
		if empty == 0 {
			v4EndWriteUnchanged(b, ctrl)
			continue
		}
		lane := v4FirstMarkedLane(empty)
		dst := table.entry(bi, lane)
		dst.WriteUnfenced(e)
		v4StoreTag(b, lane, tag)
		v4EndWriteModified(b, ctrl)
		return probe
	}
	panic("cc: V4Map grow produced a full table")
}

func v4BeginWrite(b *v4Bucket) (uint32, v4Status) {
	ctrl := b.ctrl.Load()
	return v4BeginWriteWithCtrl(b, ctrl)
}

func v4BeginWriteWithCtrl(b *v4Bucket, ctrl uint32) (uint32, v4Status) {
	if ctrl&v4FrozenMask != 0 {
		return 0, v4Frozen
	}
	if ctrl&v4WritingMask != 0 {
		return 0, v4Retry
	}
	if !b.ctrl.CompareAndSwap(ctrl, ctrl|v4WritingMask) {
		return 0, v4Retry
	}
	return ctrl, v4OK
}

func v4EndWriteUnchanged(b *v4Bucket, ctrl uint32) {
	b.ctrl.Store(ctrl)
}

func v4EndWriteModified(b *v4Bucket, ctrl uint32) {
	b.ctrl.Store(v4BumpCtrl(ctrl))
}

func v4FreezeAndLoadTags(b *v4Bucket) uint32 {
	for {
		ctrl := b.ctrl.Load()
		if ctrl&v4FrozenMask != 0 {
			return v4LoadTagWords(b)
		}
		if ctrl&v4WritingMask != 0 {
			continue
		}
		if b.ctrl.CompareAndSwap(ctrl, ctrl|v4WritingMask) {
			b.ctrl.Store(v4BumpCtrl(ctrl | v4FrozenMask))
			return v4LoadTagWords(b)
		}
	}
}

// v4BumpCtrl increments the version portion of the control word.
// Note: We do not need to clear v4WritingMask here because the returned
// ctrl from beginWrite does not have the writing mask set, and we just
// add v4VersionInc to it.
//
//go:nosplit
func v4BumpCtrl(ctrl uint32) uint32 {
	return ctrl + v4VersionInc
}

//go:nosplit
func v4LoadTagWords(b *v4Bucket) uint32 {
	return b.tags.Load()
}

//go:nosplit
func v4StoreTag(b *v4Bucket, lane uintptr, tag uint8) {
	shift := (lane & (v4SlotsPerBucket - 1)) << 3
	mask := uint32(0xff) << shift
	b.tags.Store((b.tags.Load() &^ mask) | uint32(tag)<<shift)
}

//go:nosplit
func v4MatchBits(words uint32, tag uint8) uint32 {
	// The match path verifies candidate lanes with the full key after taking a
	// ctrl snapshot, so the fast SWAR zero check may return harmless false
	// positives. Empty/deleted checks below need the exact variant because they
	// control probe termination and tombstone reuse.
	return v4MaybeZeroByteBits(words ^ v4BroadcastTag(tag))
}

//go:nosplit
func v4EmptyBits(words uint32) uint32 {
	return v4ZeroByteBits(words)
}

//go:nosplit
func v4DeletedBits(words uint32) uint32 {
	return v4ZeroByteBits(words ^ v4BroadcastTag(v4TagDeleted))
}

//go:nosplit
func v4FullBits(words uint32) uint32 {
	// Full tags use the h2 byte format and always carry the high bit. Empty (0)
	// and deleted (1) never do, so a high-bit mask is enough here.
	return words & v4LaneMarkerMask
}

//go:nosplit
func v4BroadcastTag(tag uint8) uint32 {
	return uint32(tag) * 0x01010101
}

//go:nosplit
func v4MaybeZeroByteBits(words uint32) uint32 {
	const lowBits = uint32(0x01010101)
	return (words - lowBits) &^ words & v4LaneMarkerMask
}

//go:nosplit
func v4ZeroByteBits(words uint32) uint32 {
	const lowBits = uint32(0x7f7f7f7f)
	return ^(((words & lowBits) + lowBits) | words | lowBits) & v4LaneMarkerMask
}

//go:nosplit
func v4FirstMarkedLane(marked uint32) uintptr {
	return uintptr(bits.TrailingZeros32(marked)) >> 3
}
