//go:build !race && amd64 && goexperiment.simd

package cc

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"simd/archsimd"
	"sync/atomic"
	"unsafe"
)

const (
	// v28EnableIntKey enables the specialized integer hash/start path.
	v28EnableIntKey = true
	// v28EnableDedupVal skips publishing an update when the new value equals
	// the existing value and a value equality function is available.
	v28EnableDedupVal = true
	// v28EnableStoreInGrow lets writers keep retrying against the old table
	// while a resize leader is allocating/publishing the next table. Keep it
	// off by default so writers join cooperative resize instead of extending
	// contention on old buckets.
	v28EnableStoreInGrow = false
	// v28EnableAggressiveGrow adds one extra size class on probe-limit resize
	// or observed concurrent insert pressure during table allocation.
	v28EnableAggressiveGrow = true
	// v28EnableSameKeyTombstoneReuse lets a Store revive a tombstone left by
	// the same key. When disabled, deletes clear both key and value while the
	// tombstone remains as a probe-continuation marker until resize compaction.
	v28EnableSameKeyTombstoneReuse = true
	// v28EnableAutoWyHash replaces Go's built-in hasher for safe non-integer
	// key shapes where wyHash preserves == semantics.
	v28EnableAutoWyHash = true
)

const (
	v28MinBuckets     = 8
	v28SlotsPerBucket = 28
	v28LoadFactor     = 0.9375
	v28LaneMask       = uint32(1)<<v28SlotsPerBucket - 1
)

const (
	v28TagEmpty   = uint8(0)
	v28TagDeleted = uint8(1)
)

const (
	// V28 previously experimented with an overflow/probe-continuation hint in
	// ctrl. It helped some miss-heavy probes terminate earlier, but displaced
	// inserts had to update shared bucket metadata, adding atomic writes and
	// cache-line ownership traffic. Keep ctrl for version/frozen/writing only;
	// if miss-heavy workloads need a hint again, prefer a side structure.
	v28WritingMask = uint32(1) << 0
	v28FrozenMask  = uint32(1) << 1
	v28VersionMask = uint32(0xFFFFFFFC)
	v28VersionInc  = uint32(1) << 2
)

const (
	// occupied tracks physical lanes that are no longer Empty: Full + Tombstone.
	// tombstones tracks lanes currently in Deleted/Tombstone state, not total
	// delete operations.
	v28CntOccupied = iota
	v28CntTombstones
)

type v28Status uint8

const (
	v28OK v28Status = iota
	v28Full
	v28Retry
	v28Frozen
)

type v28ResizeHint uint8

const (
	v28ResizeNormal v28ResizeHint = iota
	v28ResizeProbeLimit
	v28ResizeCompact
)

// V28Map is an experimental SIMD-probed open-addressed map.
//
// Each 32-byte bucket stores 28 one-byte tags plus a compact control word.
// Entries are kept in a separate flat array and addressed by bucket/lane,
// so probing stays cache-dense while key/value storage remains contiguous.
// Reads use a bucket version snapshot; writes publish through a short
// per-bucket writing window, and resize freezes old buckets cooperatively.
//
// Integer keys use the fast integer hash path. For string keys and key types
// that Go marks as regular memory, V28Map automatically uses an internal
// wyhash-based hasher to avoid Go's AES-based built-in hash path, which can
// contend with SIMD probing on some CPUs. Other comparable key shapes keep Go's
// built-in hasher to preserve == semantics. Use [WithKeyHasherUnsafe] to
// supply other non-AES hashers for custom key types.
type V28Map[K comparable, V any] struct {
	_         noCopy
	table     atomic.Pointer[v28Table[K, V]]
	initState atomic.Uint32
	intKey    bool
	seed      uintptr
	keyHash   HashFunc
	valEqual  EqualFunc
	minLen    uintptr
	size      PLocalCounterN
}

type v28Table[K comparable, V any] struct {
	buckets       unsafeSlice[v28Bucket]
	entries       unsafeSlice[v28Entry[K, V]]
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
	nextTable     atomic.Pointer[v28Table[K, V]]
	bucketBacking unsafe.Pointer
}

// v28Bucket stores the tags and control word for a bucket.
// Layout: 28 bytes for 28 slots of tags, 4 bytes for control metadata.
// Total 32 bytes.
// Alignment: 32-byte aligned (manually aligned via makeV28Buckets function).
// Padding: Perfectly packed to exactly 32 bytes, which aligns optimally with
// AVX2 vector registers and cache line half-blocks. 0 bytes of padding.
//
// ┌───────────────────────────────────────────────────────────────────┐
// │                   28 × 8-bit h2 tags (224 bits)                   │
// │                            bytes 0-27                             │
// ├──────────────────────────────────────────────┬──────┬─────────────┤
// │              version (30 bits)               │frozen│   writing   │
// │                  bits 31-2                   │bit 1 │    bit 0    │
// └──────────────────────────────────────────────┴──────┴─────────────┘
type v28Bucket struct {
	tags [28]byte
	ctrl atomic.Uint32
}

type v28Entry[K comparable, V any] struct {
	key K
	val V
}

func NewV28Map[K comparable, V any](options ...func(*MapConfig)) *V28Map[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m := &V28Map[K, V]{}
	m.init(noEscape(&cfg))
	return m
}

func (m *V28Map[K, V]) init(cfg *MapConfig) {
	if !archsimd.X86.AVX2() {
		panic("cc: V28Map requires AVX2")
	}
	if cfg.keyHash == nil {
		cfg.keyHash = parseKeyInterface[K]()
	}
	if cfg.valEqual == nil {
		cfg.valEqual = parseValueInterface[V]()
	}
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	m.intKey = v28EnableIntKey && m.intKey
	if cfg.keyHash != nil {
		m.keyHash = cfg.keyHash
		m.intKey = false
	} else if !m.intKey {
		if keyHash := v28AutoWyHash[K](); keyHash != nil {
			m.keyHash = keyHash
		}
	}
	if cfg.valEqual != nil {
		m.valEqual = cfg.valEqual
	}
	m.seed = uintptr(rand.Uint64())
	m.minLen = v28CalcBucketLen(cfg.capacity)
	m.table.Store(newV28Table[K, V](m.minLen, m.intKey))
}

func (m *V28Map[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	hash := m.hashKey(noEscape(&key))
	tag, start := v28HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
		// spins := 0
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v28WritingMask != 0 {
			// delay(&spins)
			goto retryBucket
		}
		words := v28LoadTagWords(b)
		match := v28MatchBits(words, tag)
		for match != 0 {
			lane := uintptr(bits.TrailingZeros32(match))
			e := table.entry(bi, lane)
			k, v := e.key, e.val
			ctrl2 := b.ctrl.Load()
			if ctrl != ctrl2 {
				goto retryBucket
			}
			if k == key {
				return v, true
			}
			match &= match - 1
		}
		if v28EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return *new(V), false
		}
	}
	return *new(V), false
}

func (m *V28Map[K, V]) Store(key K, value V) {
	m.store(noEscape(&key), noEscape(&value), false)
}

func (m *V28Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.store(noEscape(&key), noEscape(&value), true)
}

func (m *V28Map[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	return m.update(noEscape(&key), noEscape(&value), false)
}

func (m *V28Map[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.delete(noEscape(&key), true)
}

func (m *V28Map[K, V]) Delete(key K) {
	m.delete(noEscape(&key), false)
}

func (m *V28Map[K, V]) CompareAndSwap(key K, old V, new V) bool {
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
		case v28OK:
			return swapped
		case v28Frozen:
			table = m.helpResize(table)
		case v28Full:
			return false
		}
	}
}

func (m *V28Map[K, V]) CompareAndDelete(key K, old V) bool {
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
		case v28OK:
			return deleted
		case v28Frozen:
			table = m.helpResize(table)
		case v28Full:
			return false
		}
	}
}

func (m *V28Map[K, V]) Compute(key K, fn func(e *MapEntry[K, V])) (actual V, loaded bool) {
	table := m.ensureTable()
	hash := m.hashKey(noEscape(&key))
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.computeIn(table, noEscape(&key), hash, fn)
		switch status {
		case v28OK:
			if !loaded && shouldCheckResize && int(m.size.Get(v28CntOccupied)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v28Full:
			table = m.tryResize(table, int(m.size.Value(v28CntOccupied)), v28ResizeProbeLimit)
		case v28Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V28Map[K, V]) Range(yield func(K, V) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}

	type rangeEntry[K comparable, V any] struct {
		key K
		val V
	}

	var cache [v28SlotsPerBucket]rangeEntry[K, V]
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
	retry:
		ctrl := b.ctrl.Load()
		if ctrl&v28WritingMask != 0 {
			goto retry
		}
		words := v28LoadTagWords(b)
		full := v28FullBits(words)
		cacheCount := 0
		for full != 0 {
			lane := uintptr(bits.TrailingZeros32(full))
			e := table.entry(i, lane)
			cache[cacheCount] = rangeEntry[K, V]{key: e.key, val: e.val}
			cacheCount++
			full &= full - 1
		}
		ctrl2 := b.ctrl.Load()
		if ctrl != ctrl2 {
			goto retry
		}
		for j := 0; j < cacheCount; j++ {
			if !yield(cache[j].key, cache[j].val) {
				return
			}
		}
	}
}

func (m *V28Map[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

type v28MapStats struct {
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

func (m *V28Map[K, V]) stats() v28MapStats {
	table := m.table.Load()
	occupied := m.size.Value(v28CntOccupied)
	tombstones := m.size.Value(v28CntTombstones)
	stats := v28MapStats{
		Live:       occupied - tombstones,
		Occupied:   occupied,
		Tombstones: tombstones,
	}
	if table == nil {
		return stats
	}
	stats.Buckets = table.bucketLen()
	stats.Capacity = stats.Buckets * v28SlotsPerBucket
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
		words := v28LoadTagWords(b)
		full := v28FullBits(words)
		empty := v28EmptyBits(words)
		tombstones := v28DeletedBits(words)
		fullCount := uintptr(bits.OnesCount32(full))
		stats.FullLanes += fullCount
		stats.EmptyLanes += uintptr(bits.OnesCount32(empty))
		stats.TombstoneLanes += uintptr(bits.OnesCount32(tombstones))
		if fullCount == v28SlotsPerBucket {
			stats.FullBuckets++
		}
		fullScan := full
		for fullScan != 0 {
			lane := uintptr(bits.TrailingZeros32(fullScan))
			e := table.entry(i, lane)
			hash := m.hashKey(noEscape(&e.key))
			_, start := v28HashParts(hash, table.intKey, table.mask)
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

func (m *V28Map[K, V]) Size() int {
	occupied := int(m.size.Value(v28CntOccupied))
	tombstones := int(m.size.Value(v28CntTombstones))
	return max(occupied-tombstones, 0)
}

func (m *V28Map[K, V]) Clear() {
	if m.table.Load() == nil {
		return
	}
	m.size.Clear()
	m.table.Store(newV28Table[K, V](m.minLen, m.intKey))
}

func (m *V28Map[K, V]) store(key *K, val *V, onlyIfAbsent bool) (actual V, loaded bool) {
	table := m.ensureTable()
	hash := m.hashKey(key)
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.storeIn(table, key, val, hash, onlyIfAbsent)
		switch status {
		case v28OK:
			if !loaded && shouldCheckResize && int(m.size.Get(v28CntOccupied)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v28Full:
			table = m.tryResize(table, int(m.size.Value(v28CntOccupied)), v28ResizeProbeLimit)
		case v28Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V28Map[K, V]) update(key *K, val *V, onlyIfAbsent bool) (previous V, loaded bool) {
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
		case v28OK:
			return previous, loaded
		case v28Full:
			return *new(V), false
		case v28Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V28Map[K, V]) storeIn(
	table *v28Table[K, V],
	key *K,
	val *V,
	hash uintptr,
	onlyIfAbsent bool,
) (v28Status, V, bool, bool) {
	tag, start := v28HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v28FrozenMask != 0 {
			return v28Frozen, *new(V), false, false
		}
		if ctrl&v28WritingMask != 0 {
			goto retryBucket
		}
		words := v28LoadTagWords(b)
		match := v28MatchBits(words, tag)
		for match != 0 {
			lane := uintptr(bits.TrailingZeros32(match))
			e := table.entry(bi, lane)
			k, v := e.key, e.val
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if k == *key {
				if onlyIfAbsent {
					return v28OK, v, true, false
				}
				if v28EnableDedupVal && m.valEqual != nil && m.valEqual(noescape(unsafe.Pointer(&v)), noescape(unsafe.Pointer(val))) {
					return v28OK, v, true, false
				}
				ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
				if status != v28OK {
					if status == v28Retry {
						goto retryBucket
					}
					return status, *new(V), false, false
				}
				e.val = *val
				v28EndWriteModified(b, ctrl)
				return v28OK, *val, true, false
			}
			match &= match - 1
		}
		if v28EnableSameKeyTombstoneReuse {
			tombstones := v28DeletedBits(words)
			for tombstones != 0 {
				lane := uintptr(bits.TrailingZeros32(tombstones))
				e := table.entry(bi, lane)
				k := e.key
				if ctrl != b.ctrl.Load() {
					goto retryBucket
				}
				if k == *key {
					ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
					if status != v28OK {
						if status == v28Retry {
							goto retryBucket
						}
						return status, *new(V), false, false
					}
					e.val = *val
					v28StoreTag(b, lane, tag)
					v28EndWriteModified(b, ctrl)
					m.size.Add(v28CntTombstones, ^uintptr(0))
					return v28OK, *val, false, false
				}
				tombstones &= tombstones - 1
			}
		}
		if empty := v28EmptyBits(words); empty != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
			if status != v28OK {
				if status == v28Retry {
					goto retryBucket
				}
				return status, *new(V), false, false
			}
			lane := uintptr(bits.TrailingZeros32(empty))
			e := table.entry(bi, lane)
			e.key = *key
			e.val = *val
			v28StoreTag(b, lane, tag)
			v28EndWriteModified(b, ctrl)
			m.size.Add(v28CntOccupied, 1)
			return v28OK, *val, false, empty&(empty-1) == 0
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v28Full, *new(V), false, false
}

func (m *V28Map[K, V]) updateIn(
	table *v28Table[K, V],
	key *K,
	val *V,
	hash uintptr,
	onlyIfAbsent bool,
) (v28Status, V, bool) {
	tag, start := v28HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v28FrozenMask != 0 {
			return v28Frozen, *new(V), false
		}
		if ctrl&v28WritingMask != 0 {
			goto retryBucket
		}
		words := v28LoadTagWords(b)
		match := v28MatchBits(words, tag)
		for match != 0 {
			lane := uintptr(bits.TrailingZeros32(match))
			e := table.entry(bi, lane)
			k, previous := e.key, e.val
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if k == *key {
				if onlyIfAbsent {
					return v28OK, previous, true
				}
				if v28EnableDedupVal && m.valEqual != nil && m.valEqual(noescape(unsafe.Pointer(&previous)), noescape(unsafe.Pointer(val))) {
					return v28OK, previous, true
				}
				ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
				if status != v28OK {
					if status == v28Retry {
						goto retryBucket
					}
					return status, *new(V), false
				}
				e.val = *val
				v28EndWriteModified(b, ctrl)
				return v28OK, previous, true
			}
			match &= match - 1
		}
		if v28EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v28OK, *new(V), false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v28OK, *new(V), false
}

func (m *V28Map[K, V]) delete(key *K, needValue bool) (previous V, loaded bool) {
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
		case v28OK:
			return previous, loaded
		case v28Full:
			return *new(V), false
		case v28Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V28Map[K, V]) deleteIn(
	table *v28Table[K, V],
	key *K,
	hash uintptr,
	needValue bool,
) (v28Status, V, bool) {
	tag, start := v28HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v28FrozenMask != 0 {
			return v28Frozen, *new(V), false
		}
		if ctrl&v28WritingMask != 0 {
			goto retryBucket
		}
		words := v28LoadTagWords(b)
		match := v28MatchBits(words, tag)
		for match != 0 {
			lane := uintptr(bits.TrailingZeros32(match))
			e := table.entry(bi, lane)
			k, v := e.key, e.val
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if k == *key {
				ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
				if status != v28OK {
					if status == v28Retry {
						goto retryBucket
					}
					return status, *new(V), false
				}
				var prev V
				if needValue {
					prev = v
				}
				if !v28EnableSameKeyTombstoneReuse {
					e.key = *new(K)
				}
				e.val = *new(V)
				v28StoreTag(b, lane, v28TagDeleted)
				v28EndWriteModified(b, ctrl)
				m.size.Add(v28CntTombstones, 1)
				return v28OK, prev, true
			}
			match &= match - 1
		}
		if v28EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v28OK, *new(V), false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v28Full, *new(V), false
}

func (m *V28Map[K, V]) compareAndSwapIn(
	table *v28Table[K, V],
	key *K,
	hash uintptr,
	old *V,
	new *V,
) (v28Status, bool) {
	tag, start := v28HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v28FrozenMask != 0 {
			return v28Frozen, false
		}
		if ctrl&v28WritingMask != 0 {
			goto retryBucket
		}
		words := v28LoadTagWords(b)
		match := v28MatchBits(words, tag)
		for match != 0 {
			lane := uintptr(bits.TrailingZeros32(match))
			e := table.entry(bi, lane)
			k, cur := e.key, e.val
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if k == *key {
				if !m.valEqual(noescape(unsafe.Pointer(&cur)), noescape(unsafe.Pointer(old))) {
					return v28OK, false
				}
				if m.valEqual(noescape(unsafe.Pointer(&cur)), noescape(unsafe.Pointer(new))) {
					return v28OK, true
				}
				ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
				if status != v28OK {
					if status == v28Retry {
						goto retryBucket
					}
					return status, false
				}
				e.val = *new
				v28EndWriteModified(b, ctrl)
				return v28OK, true
			}
			match &= match - 1
		}
		if v28EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v28OK, false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v28Full, false
}

func (m *V28Map[K, V]) compareAndDeleteIn(
	table *v28Table[K, V],
	key *K,
	hash uintptr,
	old *V,
) (v28Status, bool) {
	tag, start := v28HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v28FrozenMask != 0 {
			return v28Frozen, false
		}
		if ctrl&v28WritingMask != 0 {
			goto retryBucket
		}
		words := v28LoadTagWords(b)
		match := v28MatchBits(words, tag)
		for match != 0 {
			lane := uintptr(bits.TrailingZeros32(match))
			e := table.entry(bi, lane)
			k, cur := e.key, e.val
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if k == *key {
				if !m.valEqual(noescape(unsafe.Pointer(&cur)), noescape(unsafe.Pointer(old))) {
					return v28OK, false
				}
				ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
				if status != v28OK {
					if status == v28Retry {
						goto retryBucket
					}
					return status, false
				}
				if !v28EnableSameKeyTombstoneReuse {
					e.key = *new(K)
				}
				e.val = *new(V)
				v28StoreTag(b, lane, v28TagDeleted)
				v28EndWriteModified(b, ctrl)
				m.size.Add(v28CntTombstones, 1)
				return v28OK, true
			}
			match &= match - 1
		}
		if v28EmptyBits(words) != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			return v28OK, false
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v28Full, false
}

func (m *V28Map[K, V]) computeIn(
	table *v28Table[K, V],
	key *K,
	hash uintptr,
	fn func(e *MapEntry[K, V]),
) (v28Status, V, bool, bool) {
	tag, start := v28HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.ctrl.Load()
		if ctrl&v28FrozenMask != 0 {
			return v28Frozen, *new(V), false, false
		}
		if ctrl&v28WritingMask != 0 {
			goto retryBucket
		}
		words := v28LoadTagWords(b)
		match := v28MatchBits(words, tag)
		for match != 0 {
			lane := uintptr(bits.TrailingZeros32(match))
			e := table.entry(bi, lane)
			k, v := e.key, e.val
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			if k == *key {
				ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
				if status != v28OK {
					if status == v28Retry {
						goto retryBucket
					}
					return status, *new(V), false, false
				}
				it := MapEntry[K, V]{
					entry:  entry_[K, V]{hash: hash, key: *key, value: v},
					loaded: true,
				}
				fn(noEscape(&it))
				switch it.op {
				case updateOp:
					e.val = it.entry.value
					v28EndWriteModified(b, ctrl)
					return v28OK, it.entry.value, true, false
				case deleteOp:
					if !v28EnableSameKeyTombstoneReuse {
						e.key = *new(K)
					}
					e.val = *new(V)
					v28StoreTag(b, lane, v28TagDeleted)
					v28EndWriteModified(b, ctrl)
					m.size.Add(v28CntTombstones, 1)
					return v28OK, it.entry.value, true, false
				default:
					v28EndWriteUnchanged(b, ctrl)
					return v28OK, it.entry.value, true, false
				}
			}
			match &= match - 1
		}
		if v28EnableSameKeyTombstoneReuse {
			tombstones := v28DeletedBits(words)
			for tombstones != 0 {
				lane := uintptr(bits.TrailingZeros32(tombstones))
				e := table.entry(bi, lane)
				k := e.key
				if ctrl != b.ctrl.Load() {
					goto retryBucket
				}
				if k == *key {
					ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
					if status != v28OK {
						if status == v28Retry {
							goto retryBucket
						}
						return status, *new(V), false, false
					}
					it := MapEntry[K, V]{
						entry: entry_[K, V]{hash: hash, key: *key},
					}
					fn(noEscape(&it))
					if it.op != updateOp {
						v28EndWriteUnchanged(b, ctrl)
						return v28OK, *new(V), false, false
					}
					e.val = it.entry.value
					v28StoreTag(b, lane, tag)
					v28EndWriteModified(b, ctrl)
					m.size.Add(v28CntTombstones, ^uintptr(0))
					return v28OK, it.entry.value, false, false
				}
				tombstones &= tombstones - 1
			}
		}
		if empty := v28EmptyBits(words); empty != 0 {
			if ctrl != b.ctrl.Load() {
				goto retryBucket
			}
			ctrl, status := v28BeginWriteWithCtrl(b, ctrl)
			if status != v28OK {
				if status == v28Retry {
					goto retryBucket
				}
				return status, *new(V), false, false
			}
			lane := uintptr(bits.TrailingZeros32(empty))
			it := MapEntry[K, V]{
				entry: entry_[K, V]{hash: hash, key: *key},
			}
			fn(noEscape(&it))
			if it.op != updateOp {
				v28EndWriteUnchanged(b, ctrl)
				return v28OK, *new(V), false, false
			}
			e := table.entry(bi, lane)
			e.key = *key
			e.val = it.entry.value
			v28StoreTag(b, lane, tag)
			v28EndWriteModified(b, ctrl)
			m.size.Add(v28CntOccupied, 1)
			return v28OK, it.entry.value, false, empty&(empty-1) == 0
		}
		if ctrl != b.ctrl.Load() {
			goto retryBucket
		}
	}
	return v28Full, *new(V), false, false
}

func (m *V28Map[K, V]) resizeIfNeeded(table *v28Table[K, V]) {
	if v28EnableStoreInGrow {
		if table.allocating.Load() != 0 {
			return
		}
	}
	occupied := int(m.size.Value(v28CntOccupied))
	if occupied >= table.growCap {
		m.tryResize(table, occupied, v28ResizeNormal)
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
	// tombstones := int(m.size.Value(v28CntTombstones))
	// live := occupied - tombstones
	// if tombstones > v28SlotsPerBucket && tombstones > live/4 {
	// 	m.tryResize(table, occupied, v28ResizeCompact)
	// }
}

func (m *V28Map[K, V]) tryResize(old *v28Table[K, V], occupied int, hint v28ResizeHint) *v28Table[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}
	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			// Base sizing follows the live entry count. At the normal grow
			// threshold this rounds to 2x; tombstone-heavy resize can stay at
			// the same size and compact tombstone slots away.
			tombstones := int(m.size.Value(v28CntTombstones))
			live := occupied - tombstones
			nextLen := v28CalcBucketLen(live)
			nextLen = max(nextLen, m.minLen)
			aggressive := hint == v28ResizeProbeLimit
			if v28EnableAggressiveGrow {
				curOccupied := int(m.size.Value(v28CntOccupied))
				occupiedInResize := curOccupied - occupied
				aggressive = aggressive || occupiedInResize >= 2
			}
			// Probe-limit resize or observed concurrent insert pressure gets
			// one extra size class, capped at 4x the old table.
			if aggressive {
				nextLen = min(nextLen<<1, old.bucketLen()<<2)
			}
			next = newV28Table[K, V](nextLen, old.intKey)
			old.nextTable.Store(next)
		} else {
			if v28EnableStoreInGrow {
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

func (m *V28Map[K, V]) helpResize(old *v28Table[K, V]) *v28Table[K, V] {
	next := old.nextTable.Load()
	if next == nil {
		return m.tryResize(old, int(m.size.Value(v28CntOccupied)), v28ResizeProbeLimit)
	}
	return m.helpResizeInto(old, next)
}

func (m *V28Map[K, V]) helpResizeInto(old, next *v28Table[K, V]) *v28Table[K, V] {
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
			words := v28FreezeAndLoadTags(b)
			full := v28FullBits(words)
			for full != 0 {
				lane := uintptr(bits.TrailingZeros32(full))
				e := old.entry(i, lane)
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
			occupied := m.size.Reset(v28CntOccupied)
			tombstones := m.size.Reset(v28CntTombstones)
			m.size.Add(v28CntOccupied, occupied-tombstones)
			m.table.CompareAndSwap(old, next)
		}
	}
}

func (m *V28Map[K, V]) ensureTable() *v28Table[K, V] {
	if table := m.table.Load(); table != nil {
		return table
	}
	return m.slowInit()
}

//go:noinline
func (m *V28Map[K, V]) slowInit() *v28Table[K, V] {
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
func (m *V28Map[K, V]) hashKey(key *K) uintptr {
	if m.intKey {
		return intHash[K](noescape(unsafe.Pointer(key)))
	}
	return m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
}

func v28AutoWyHash[K comparable]() HashFunc {
	if !v28EnableAutoWyHash {
		return nil
	}
	keyType := iTypeOf((map[K]struct{})(nil)).MapType().Key
	if keyType.Kind_&v28KindMask == v28KindString {
		return wyHashStr
	}
	if keyType.TFlag&v28TFlagRegularMem != 0 {
		return func(ptr unsafe.Pointer, seed uintptr) uintptr {
			return wyHashMem(ptr, unsafe.Sizeof(*new(K)), seed)
		}
	}
	return nil
}

func newV28Table[K comparable, V any](bucketLen uintptr, intKey bool) *v28Table[K, V] {
	bucketLen = nextPowOf2(max(bucketLen, uintptr(v28MinBuckets)))
	slotLen := bucketLen * v28SlotsPerBucket
	growCap := int(float64(slotLen) * v28LoadFactor)
	// Stripe size in PLocalCounter is runtime.GOMAXPROCS(0).
	cpus := maxProcs()
	stripeCap := max(growCap/int(cpus), 1)
	chunks, chunkSz := v28ResizeChunks(bucketLen, cpus)
	buckets, bucketBacking := makeV28Buckets(bucketLen)
	entries := makeUnsafeSlice[v28Entry[K, V]](slotLen)
	return &v28Table[K, V]{
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

func makeV28Buckets(bucketLen uintptr) (unsafeSlice[v28Bucket], unsafe.Pointer) {
	stride := unsafe.Sizeof(v28Bucket{})
	align := stride
	backing := make([]byte, bucketLen*stride+align-1)
	basePtr := unsafe.Pointer(unsafe.SliceData(backing))
	base := uintptr(basePtr)
	aligned := (base + align - 1) &^ (align - 1)
	return unsafeSlice[v28Bucket]{ptr: unsafe.Pointer(aligned)}, basePtr //nolint:all
}

//go:nosplit
func v28HashParts(hash uintptr, intKey bool, mask uintptr) (uint8, uintptr) {
	// V28 keeps separate SIMD tag bytes, so it does not need h2's high-bit full
	// marker. Use an 8-bit-ish tag (excluding empty/deleted) to reduce false
	// positives across 28 lanes. Non-integer starts use the next hash bits so
	// tag and bucket selection do not overlap.
	if intKey {
		mixed := uint64(hash) * uint64(0x9e3779b97f4a7c15)
		tag := uint8(mixed >> 56)
		if tag < 2 {
			tag += 2
		}
		return tag, uintptr(mixed>>32) & mask
	}
	tag := uint8(hash)
	if tag < 2 {
		tag += 2
	}
	return tag, (hash >> 8) & mask
}

//go:nosplit
func v28CalcBucketLen(capacity int) uintptr {
	if capacity <= 0 {
		return v28MinBuckets
	}
	const invLoadFactor = 1 / v28LoadFactor
	needSlots := uintptr(float64(capacity+1) * invLoadFactor)
	needBuckets := (needSlots + v28SlotsPerBucket - 1) / v28SlotsPerBucket
	return nextPowOf2(max(needBuckets, uintptr(v28MinBuckets)))
}

//go:nosplit
func v28ResizeChunks(bucketLen, cpus uintptr) (chunks uint32, chunkSz uintptr) {
	const overCpus = resizeOverPartition
	want := min(bucketLen/v28MinBuckets, max(cpus*overCpus, 1))
	if want <= 1 {
		return 1, bucketLen
	}
	c := uint32(1) << (bits.Len32(uint32(want)) - 1)
	return c, bucketLen >> bits.TrailingZeros32(c)
}

//go:nosplit
func (table *v28Table[K, V]) bucketLen() uintptr {
	return table.mask + 1
}

//go:nosplit
func (table *v28Table[K, V]) entry(bucketIdx, lane uintptr) *v28Entry[K, V] {
	return table.entries.At(bucketIdx*v28SlotsPerBucket + lane)
}

func (table *v28Table[K, V]) copyInsertConcurrent(e *v28Entry[K, V], hash uintptr) uintptr {
	tag, start := v28HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
		ctrl, status := v28BeginWrite(b)
		if status != v28OK {
			probe--
			continue
		}
		words := v28LoadTagWords(b)
		empty := v28EmptyBits(words)
		if empty == 0 {
			v28EndWriteUnchanged(b, ctrl)
			continue
		}
		lane := uintptr(bits.TrailingZeros32(empty))
		dst := table.entry(bi, lane)
		dst.key = e.key
		dst.val = e.val
		v28StoreTag(b, lane, tag)
		v28EndWriteModified(b, ctrl)
		return probe
	}
	panic("cc: V28Map grow produced a full table")
}

func v28BeginWrite(b *v28Bucket) (uint32, v28Status) {
	ctrl := b.ctrl.Load()
	return v28BeginWriteWithCtrl(b, ctrl)
}

func v28BeginWriteWithCtrl(b *v28Bucket, ctrl uint32) (uint32, v28Status) {
	if ctrl&v28FrozenMask != 0 {
		return 0, v28Frozen
	}
	if ctrl&v28WritingMask != 0 {
		return 0, v28Retry
	}
	if !b.ctrl.CompareAndSwap(ctrl, ctrl|v28WritingMask) {
		return 0, v28Retry
	}
	return ctrl, v28OK
}

func v28EndWriteUnchanged(b *v28Bucket, ctrl uint32) {
	b.ctrl.Store(ctrl)
}

func v28EndWriteModified(b *v28Bucket, ctrl uint32) {
	b.ctrl.Store(v28BumpCtrl(ctrl))
}

func v28FreezeAndLoadTags(b *v28Bucket) archsimd.Uint8x32 {
	for {
		ctrl := b.ctrl.Load()
		if ctrl&v28FrozenMask != 0 {
			return v28LoadTagWords(b)
		}
		if ctrl&v28WritingMask != 0 {
			continue
		}
		if b.ctrl.CompareAndSwap(ctrl, ctrl|v28WritingMask) {
			b.ctrl.Store(v28BumpCtrl(ctrl | v28FrozenMask))
			return v28LoadTagWords(b)
		}
	}
}

// v28BumpCtrl increments the version portion of the control word.
// Note: We do not need to clear v28WritingMask here because the returned
// ctrl from beginWrite does not have the writing mask set, and we just
// add v28VersionInc to it.
//
//go:nosplit
func v28BumpCtrl(ctrl uint32) uint32 {
	return ctrl + v28VersionInc
}

//go:nosplit
func v28LoadTagWords(b *v28Bucket) archsimd.Uint8x32 {
	return archsimd.LoadUint8x32((*[32]uint8)(unsafe.Pointer(&b.tags[0])))
}

//go:nosplit
func v28StoreTag(b *v28Bucket, lane uintptr, tag uint8) {
	b.tags[lane] = tag
}

//go:nosplit
func v28MatchBits(words archsimd.Uint8x32, tag uint8) uint32 {
	return words.Equal(archsimd.BroadcastUint8x32(tag)).ToBits() & v28LaneMask
}

//go:nosplit
func v28EmptyBits(words archsimd.Uint8x32) uint32 {
	return words.Equal(archsimd.BroadcastUint8x32(v28TagEmpty)).ToBits() & v28LaneMask
}

//go:nosplit
func v28DeletedBits(words archsimd.Uint8x32) uint32 {
	return words.Equal(archsimd.BroadcastUint8x32(v28TagDeleted)).ToBits() & v28LaneMask
}

//go:nosplit
func v28FullBits(words archsimd.Uint8x32) uint32 {
	// Full lanes are encoded as tags >= 2; empty is 0 and deleted is 1.
	return words.Greater(archsimd.BroadcastUint8x32(v28TagDeleted)).ToBits() & v28LaneMask
}

// ============================================================================
// v28 Hash Utilities
// ============================================================================

const (
	v28KindMask        = iKind(0x1f)
	v28KindString      = iKind(24)
	v28TFlagRegularMem = iTFlag(1 << 3)
)

const (
	wyP0 = uint64(0xa0761d6478bd642f)
	wyP1 = uint64(0xe7037ed1a0b428db)
	wyP2 = uint64(0x8ebc6af09c88c6e3)
	wyP3 = uint64(0x589965cc75374cc3)
	wyP4 = uint64(0x1d8e4e27c47d124f)
)

//go:nosplit
func wyHashStr(ptr unsafe.Pointer, seed uintptr) uintptr {
	return uintptr(wyHashPtr(noEscape(unsafe.StringData(*(*string)(ptr))), len(*(*string)(ptr)), uint64(seed)))
}

//go:nosplit
func wyHashMem(ptr unsafe.Pointer, size uintptr, seed uintptr) uintptr {
	return uintptr(wyHashPtr((*byte)(noescape(ptr)), int(size), uint64(seed)))
}

//go:nosplit
func wyHashPtr(ptr *byte, n int, seed uint64) uint64 {
	if n == 0 {
		return seed
	}

	switch {
	case n < 4:
		return wyMul(wyMul(wyRead3(ptr, n)^seed^wyP0, seed^wyP1)^seed, uint64(n)^wyP4)
	case n <= 8:
		return wyMul(wyMul(uint64(wyRead32(ptr, 0))^seed^wyP0, uint64(wyRead32(ptr, n-4))^seed^wyP1)^seed, uint64(n)^wyP4)
	case n <= 16:
		return wyMul(wyMul(wyRead8Mix(ptr, 0)^seed^wyP0, wyRead8Mix(ptr, n-8)^seed^wyP1)^seed, uint64(n)^wyP4)
	case n <= 24:
		return wyMul(wyMul(wyRead8Mix(ptr, 0)^seed^wyP0, wyRead8Mix(ptr, 8)^seed^wyP1)^wyMul(wyRead8Mix(ptr, n-8)^seed^wyP2, seed^wyP3), uint64(n)^wyP4)
	case n <= 32:
		return wyMul(wyMul(wyRead8Mix(ptr, 0)^seed^wyP0, wyRead8Mix(ptr, 8)^seed^wyP1)^wyMul(wyRead8Mix(ptr, 16)^seed^wyP2, wyRead8Mix(ptr, n-8)^seed^wyP3), uint64(n)^wyP4)
	}

	see1 := seed
	i := 0
	for n-i > 256 {
		seed = wyMul(wyRead64(ptr, i)^seed^wyP0, wyRead64(ptr, i+8)^seed^wyP1) ^
			wyMul(wyRead64(ptr, i+16)^seed^wyP2, wyRead64(ptr, i+24)^seed^wyP3)
		see1 = wyMul(wyRead64(ptr, i+32)^see1^wyP1, wyRead64(ptr, i+40)^see1^wyP2) ^
			wyMul(wyRead64(ptr, i+48)^see1^wyP3, wyRead64(ptr, i+56)^see1^wyP0)
		seed = wyMul(wyRead64(ptr, i+64)^seed^wyP0, wyRead64(ptr, i+72)^seed^wyP1) ^
			wyMul(wyRead64(ptr, i+80)^seed^wyP2, wyRead64(ptr, i+88)^seed^wyP3)
		see1 = wyMul(wyRead64(ptr, i+96)^see1^wyP1, wyRead64(ptr, i+104)^see1^wyP2) ^
			wyMul(wyRead64(ptr, i+112)^see1^wyP3, wyRead64(ptr, i+120)^see1^wyP0)
		seed = wyMul(wyRead64(ptr, i+128)^seed^wyP0, wyRead64(ptr, i+136)^seed^wyP1) ^
			wyMul(wyRead64(ptr, i+144)^seed^wyP2, wyRead64(ptr, i+152)^seed^wyP3)
		see1 = wyMul(wyRead64(ptr, i+160)^see1^wyP1, wyRead64(ptr, i+168)^see1^wyP2) ^
			wyMul(wyRead64(ptr, i+176)^see1^wyP3, wyRead64(ptr, i+184)^see1^wyP0)
		seed = wyMul(wyRead64(ptr, i+192)^seed^wyP0, wyRead64(ptr, i+200)^seed^wyP1) ^
			wyMul(wyRead64(ptr, i+208)^seed^wyP2, wyRead64(ptr, i+216)^seed^wyP3)
		see1 = wyMul(wyRead64(ptr, i+224)^see1^wyP1, wyRead64(ptr, i+232)^see1^wyP2) ^
			wyMul(wyRead64(ptr, i+240)^see1^wyP3, wyRead64(ptr, i+248)^see1^wyP0)
		i += 256
	}

	for n-i > 32 {
		seed = wyMul(wyRead64(ptr, i)^seed^wyP0, wyRead64(ptr, i+8)^seed^wyP1)
		see1 = wyMul(wyRead64(ptr, i+16)^see1^wyP2, wyRead64(ptr, i+24)^see1^wyP3)
		i += 32
	}

	tail := n - i
	switch {
	case tail < 4:
		seed = wyMul(wyRead3At(ptr, i, tail)^seed^wyP0, seed^wyP1)
	case tail <= 8:
		seed = wyMul(uint64(wyRead32(ptr, i))^seed^wyP0, uint64(wyRead32(ptr, n-4))^seed^wyP1)
	case tail <= 16:
		seed = wyMul(wyRead8Mix(ptr, i)^seed^wyP0, wyRead8Mix(ptr, n-8)^seed^wyP1)
	case tail <= 24:
		seed = wyMul(wyRead8Mix(ptr, i)^seed^wyP0, wyRead8Mix(ptr, i+8)^seed^wyP1)
		see1 = wyMul(wyRead8Mix(ptr, n-8)^see1^wyP2, see1^wyP3)
	default:
		seed = wyMul(wyRead8Mix(ptr, i)^seed^wyP0, wyRead8Mix(ptr, i+8)^seed^wyP1)
		see1 = wyMul(wyRead8Mix(ptr, i+16)^see1^wyP2, wyRead8Mix(ptr, n-8)^see1^wyP3)
	}

	return wyMul(seed^see1, uint64(n)^wyP4)
}

//go:nosplit
func wyMul(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

//go:nosplit
func wyRead3(ptr *byte, n int) uint64 {
	return wyRead3At(ptr, 0, n)
}

//go:nosplit
func wyRead3At(ptr *byte, i int, n int) uint64 {
	return uint64(*(*byte)(unsafe.Add(unsafe.Pointer(ptr), i)))<<16 |
		uint64(*(*byte)(unsafe.Add(unsafe.Pointer(ptr), i+(n>>1))))<<8 |
		uint64(*(*byte)(unsafe.Add(unsafe.Pointer(ptr), i+n-1)))
}

//go:nosplit
func wyRead8Mix(ptr *byte, i int) uint64 {
	return uint64(wyRead32(ptr, i))<<32 | uint64(wyRead32(ptr, i+4))
}

//go:nosplit
func wyRead64(ptr *byte, i int) uint64 {
	return *(*uint64)(unsafe.Add(unsafe.Pointer(ptr), i))
}

//go:nosplit
func wyRead32(ptr *byte, i int) uint32 {
	return *(*uint32)(unsafe.Add(unsafe.Pointer(ptr), i))
}
