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
	// v6EnableIntKey enables the specialized integer hash/start path.
	v6EnableIntKey = true
	// v6EnableDedupVal skips publishing an update when the new value equals
	// the existing value and a value equality function is available.
	v6EnableDedupVal = true
	// v6EnableStoreInGrow lets writers keep retrying against the old table
	// while a resize leader is allocating/publishing the next table. Keep it
	// off by default so writers join cooperative resize instead of extending
	// contention on old buckets.
	v6EnableStoreInGrow = false
	// v6EnableAggressiveGrow adds one extra size class on probe-limit resize
	// or observed concurrent insert pressure during table allocation.
	v6EnableAggressiveGrow = true
	// v6EnableSameKeyTombstoneReuse lets a Store revive a tombstone left by
	// the same key. When disabled, deletes clear both key and value while the
	// tombstone remains as a probe-continuation marker until resize compaction.
	v6EnableSameKeyTombstoneReuse = true
)

const (
	v6MinBuckets      = 32
	v6SlotsPerBucket  = 6
	v6LoadFactorNum   = 12
	v6LoadFactorDen   = 16
	v6MaxProbeBuckets = 32
	v6LaneMarkerMask  = uint64(0x808080808080)
)

const (
	v6TagEmpty   = uint8(0)
	v6TagDeleted = uint8(1)
)

const (
	// Keep ctrl for version/frozen/writing only. If miss-heavy workloads need a
	// probe-continuation hint, prefer a side structure so displaced inserts do not
	// have to update shared bucket metadata.
	v6VersionMask = /* unused but kept for ref */ uint64(0x3fff) << 50
	v6FrozenMask  = uint64(1) << 49
	v6WritingMask = uint64(1) << 48
	v6VersionInc  = uint64(1) << 50
)

const (
	v6CntUsed = iota
	v6CntDeleted
)

type v6Status uint8

const (
	v6OK v6Status = iota
	v6Full
	v6Retry
	v6Frozen
)

type v6ResizeHint uint8

const (
	v6ResizeNormal v6ResizeHint = iota
	v6ResizeProbeLimit
	v6ResizeCompact
)

// V6Map is an experimental SWAR-probed open-addressed map.
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
type V6Map[K comparable, V any] struct {
	_         noCopy
	table     atomic.Pointer[v6Table[K, V]]
	initState atomic.Uint32
	intKey    bool
	seed      uintptr
	keyHash   HashFunc
	valEqual  EqualFunc
	minLen    uintptr
	size      PLocalCounterN
}

type v6Table[K comparable, V any] struct {
	buckets      unsafeSlice[v6Bucket]
	entries      unsafeSlice[SeqLockSlot[v6Entry[K, V]]]
	mask         uintptr
	probeLimit   uintptr
	stripeCap    int
	growCap      int
	intKey       bool
	chunks       uint32
	chunkSz      uintptr
	allocating   atomic.Uint32
	copyIdx      atomic.Uint32
	copyDone     atomic.Uint32
	copyMaxProbe atomic.Uintptr
	nextTable    atomic.Pointer[v6Table[K, V]]
}

// v6Bucket stores the tags and control metadata unified in a single 64-bit word.
// Layout: 6 bytes (48 bits) for tags, 2 bytes (16 bits) for control metadata.
// Total 8 bytes.
// Alignment: 8-byte aligned (due to atomic.Uint64).
// Padding: Perfectly packed, 0 bytes of padding.
//
// ABA Resistance: By packing the 14-bit version counter and 48-bit tags into a
// single 64-bit state, reads and CAS operations are strictly tear-free. Even
// though the version counter wraps after 16,384 writes, an ABA requires the
// entire 64-bit state to match. This yields an effective ~62-bit ABA resistance,
// because both the version counter MUST wrap AND the exact 48-bit tag layout
// MUST be perfectly restored simultaneously. This is structurally safer than
// splitting tags and control words across separate atomic loads.
//
// ┌───────────────────┬──────┬───────┬─────────────────────────────────────┐
// │ version (14 bits) │frozen│writing│     6 × 8-bit h2 tags (48 bits)     │
// │    bits 63-50     │bit 49│bit 48 │              bits 47-0              │
// └───────────────────┴──────┴───────┴─────────────────────────────────────┘
type v6Bucket struct {
	state atomic.Uint64
}

type v6Entry[K comparable, V any] struct {
	key K
	val V
}

func NewV6Map[K comparable, V any](options ...func(*MapConfig)) *V6Map[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m := &V6Map[K, V]{}
	m.init(noEscape(&cfg))
	return m
}

func (m *V6Map[K, V]) init(cfg *MapConfig) {
	if cfg.keyHash == nil {
		cfg.keyHash = parseKeyInterface[K]()
	}
	if cfg.valEqual == nil {
		cfg.valEqual = parseValueInterface[V]()
	}
	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	m.intKey = v6EnableIntKey && m.intKey
	if cfg.keyHash != nil {
		m.keyHash = cfg.keyHash
		m.intKey = false
	}
	if cfg.valEqual != nil {
		m.valEqual = cfg.valEqual
	}
	m.seed = uintptr(rand.Uint64())
	m.minLen = v6CalcBucketLen(cfg.capacity)
	m.table.Store(newV6Table[K, V](m.minLen, m.intKey))
}

func (m *V6Map[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	hash := m.hashKey(noEscape(&key))
	tag, start := v6HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6WritingMask != 0 {
			goto retryBucket
		}
		words := v6LoadTagWords(b)
		match := v6MatchBits(words, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			e := table.entry(bi, lane).ReadUnfenced()
			ctrl2 := b.state.Load()
			if ctrl != ctrl2 || ctrl2&v6WritingMask != 0 {
				goto retryBucket
			}
			if e.key == key {
				return e.val, true
			}
			match &= match - 1
		}
		if v6EmptyBits(words) != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			return *new(V), false
		}
	}
	return *new(V), false
}

func (m *V6Map[K, V]) Store(key K, value V) {
	m.store(noEscape(&key), noEscape(&value), false)
}

func (m *V6Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.store(noEscape(&key), noEscape(&value), true)
}

func (m *V6Map[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	return m.update(noEscape(&key), noEscape(&value), false)
}

func (m *V6Map[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.delete(noEscape(&key), true)
}

func (m *V6Map[K, V]) Delete(key K) {
	m.delete(noEscape(&key), false)
}

func (m *V6Map[K, V]) CompareAndSwap(key K, old V, new V) bool {
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
		case v6OK:
			return swapped
		case v6Frozen:
			table = m.helpResize(table)
		case v6Retry:
			runtime.Gosched()
		case v6Full:
			table = m.tryResize(table, int(m.size.Value(v6CntUsed)), v6ResizeProbeLimit)
		}
	}
}

func (m *V6Map[K, V]) CompareAndDelete(key K, old V) bool {
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
		case v6OK:
			return deleted
		case v6Frozen:
			table = m.helpResize(table)
		case v6Retry:
			runtime.Gosched()
		case v6Full:
			table = m.tryResize(table, int(m.size.Value(v6CntUsed)), v6ResizeProbeLimit)
		}
	}
}

func (m *V6Map[K, V]) Compute(key K, fn func(e *MapEntry[K, V])) (actual V, loaded bool) {
	table := m.ensureTable()
	hash := m.hashKey(noEscape(&key))
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.computeIn(table, noEscape(&key), hash, fn)
		switch status {
		case v6OK:
			if !loaded && shouldCheckResize && int(m.size.Get(v6CntUsed)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v6Full:
			table = m.tryResize(table, int(m.size.Value(v6CntUsed)), v6ResizeProbeLimit)
		case v6Frozen:
			table = m.helpResize(table)
		case v6Retry:
			runtime.Gosched()
		}
	}
}

func (m *V6Map[K, V]) Range(yield func(K, V) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}

	var cache [v6SlotsPerBucket]v6Entry[K, V]
	unsafeCache := toUnsafeSlice(&cache[0])
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
	retry:
		ctrl := b.state.Load()
		if ctrl&v6WritingMask != 0 {
			goto retry
		}
		words := v6LoadTagWords(b)
		full := v6FullBits(words)
		var cacheCount uintptr
		for full != 0 {
			lane := v6FirstMarkedLane(full)
			*unsafeCache.At(cacheCount) = table.entry(i, lane).ReadUnfenced()
			cacheCount++
			full &= full - 1
		}
		ctrl2 := b.state.Load()
		if ctrl != ctrl2 || ctrl2&v6WritingMask != 0 {
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

func (m *V6Map[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

type v6MapStats struct {
	Buckets        uintptr
	Capacity       uintptr
	Live           uintptr
	Used           uintptr
	Deleted        uintptr
	FullBuckets    uintptr
	FullLanes      uintptr
	EmptyLanes     uintptr
	TombstoneLanes uintptr
	MaxProbe       uintptr
	ProbeTotal     uintptr
	ProbeSamples   uintptr
}

func (m *V6Map[K, V]) stats() v6MapStats {
	table := m.table.Load()
	used := m.size.Value(v6CntUsed)
	deleted := m.size.Value(v6CntDeleted)
	stats := v6MapStats{
		Live:    used - deleted,
		Used:    used,
		Deleted: deleted,
	}
	if table == nil {
		return stats
	}
	stats.Buckets = table.bucketLen()
	stats.Capacity = stats.Buckets * v6SlotsPerBucket
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
		words := v6LoadTagWords(b)
		full := v6FullBits(words)
		empty := v6EmptyBits(words)
		deleted := v6DeletedBits(words)
		fullCount := uintptr(bits.OnesCount64(full))
		stats.FullLanes += fullCount
		stats.EmptyLanes += uintptr(bits.OnesCount64(empty))
		stats.TombstoneLanes += uintptr(bits.OnesCount64(deleted))
		if fullCount == v6SlotsPerBucket {
			stats.FullBuckets++
		}
		fullScan := full
		for fullScan != 0 {
			lane := v6FirstMarkedLane(fullScan)
			e := table.entry(i, lane).ReadUnfenced()
			hash := m.hashKey(noEscape(&e.key))
			_, start := v6HashParts(hash, table.intKey, table.mask)
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

func (m *V6Map[K, V]) Size() int {
	used := int(m.size.Value(v6CntUsed))
	deleted := int(m.size.Value(v6CntDeleted))
	return max(used-deleted, 0)
}

func (m *V6Map[K, V]) Clear() {
	if m.table.Load() == nil {
		return
	}
	m.size.Clear()
	m.table.Store(newV6Table[K, V](m.minLen, m.intKey))
}

func (m *V6Map[K, V]) store(key *K, val *V, onlyIfAbsent bool) (actual V, loaded bool) {
	table := m.ensureTable()
	hash := m.hashKey(key)
	for {
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.storeIn(table, key, val, hash, onlyIfAbsent)
		switch status {
		case v6OK:
			if !loaded && shouldCheckResize && int(m.size.Get(v6CntUsed)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v6Full:
			table = m.tryResize(table, int(m.size.Value(v6CntUsed)), v6ResizeProbeLimit)
		case v6Frozen:
			table = m.helpResize(table)
		case v6Retry:
			runtime.Gosched()
		}
	}
}

func (m *V6Map[K, V]) update(key *K, val *V, onlyIfAbsent bool) (previous V, loaded bool) {
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
		case v6OK:
			if !loaded && shouldCheckResize && int(m.size.Get(v6CntUsed)) >= table.stripeCap {
				m.resizeIfNeeded(table)
			}
			return previous, loaded
		case v6Full:
			table = m.tryResize(table, int(m.size.Value(v6CntUsed)), v6ResizeProbeLimit)
		case v6Frozen:
			table = m.helpResize(table)
		case v6Retry:
			runtime.Gosched()
		}
	}
}

func (m *V6Map[K, V]) storeIn(
	table *v6Table[K, V],
	key *K,
	val *V,
	hash uintptr,
	onlyIfAbsent bool,
) (v6Status, V, bool, bool) {
	tag, start := v6HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, *new(V), false, false
		}
		if ctrl&v6WritingMask != 0 {
			return v6Retry, *new(V), false, false
		}
		words := v6LoadTagWords(b)
		match := v6MatchBits(words, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if onlyIfAbsent {
					return v6OK, e.val, true, false
				}
				if v6EnableDedupVal && m.valEqual != nil &&
					m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return v6OK, e.val, true, false
				}
				ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
				if status != v6OK {
					return status, *new(V), false, false
				}
				slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: *val})
				v6EndWriteModified(b, ctrl)
				return v6OK, *val, true, false
			}
			match &= match - 1
		}
		if v6EnableSameKeyTombstoneReuse {
			deleted := v6DeletedBits(words)
			for deleted != 0 {
				lane := v6FirstMarkedLane(deleted)
				slot := table.entry(bi, lane)
				e := slot.ReadUnfenced()
				if ctrl != b.state.Load() {
					goto retryBucket
				}
				if e.key == *key {
					ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
					if status != v6OK {
						return status, *new(V), false, false
					}
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: *val})
					ctrl = v6SetTag(ctrl, lane, tag)
					v6EndWriteModified(b, ctrl)
					m.size.Add(v6CntDeleted, ^uintptr(0))
					return v6OK, *val, false, false
				}
				deleted &= deleted - 1
			}
		}
		if empty := v6EmptyBits(words); empty != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
			if status != v6OK {
				return status, *new(V), false, false
			}
			lane := v6FirstMarkedLane(empty)
			table.entry(bi, lane).WriteUnfenced(v6Entry[K, V]{key: *key, val: *val})
			ctrl = v6SetTag(ctrl, lane, tag)
			v6EndWriteModified(b, ctrl)
			m.size.Add(v6CntUsed, 1)
			return v6OK, *val, false, empty&(empty-1) == 0
		}
		if ctrl != b.state.Load() {
			goto retryBucket
		}
	}
	return v6Full, *new(V), false, false
}

func (m *V6Map[K, V]) updateIn(
	table *v6Table[K, V],
	key *K,
	val *V,
	hash uintptr,
	onlyIfAbsent bool,
) (v6Status, V, bool, bool) {
	tag, start := v6HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, *new(V), false, false
		}
		if ctrl&v6WritingMask != 0 {
			return v6Retry, *new(V), false, false
		}
		words := v6LoadTagWords(b)
		match := v6MatchBits(words, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if onlyIfAbsent {
					return v6OK, e.val, true, false
				}
				if v6EnableDedupVal && m.valEqual != nil &&
					m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return v6OK, e.val, true, false
				}
				ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
				if status != v6OK {
					return status, *new(V), false, false
				}
				slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: *val})
				v6EndWriteModified(b, ctrl)
				return v6OK, e.val, true, false
			}
			match &= match - 1
		}
		if v6EmptyBits(words) != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			return v6OK, *new(V), false, false
		}
		if ctrl != b.state.Load() {
			goto retryBucket
		}
	}
	return v6OK, *new(V), false, false
}

func (m *V6Map[K, V]) delete(key *K, needValue bool) (previous V, loaded bool) {
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
		case v6OK:
			return previous, loaded
		case v6Full:
			return *new(V), false
		case v6Frozen:
			table = m.helpResize(table)
		case v6Retry:
			runtime.Gosched()
		}
	}
}

func (m *V6Map[K, V]) deleteIn(
	table *v6Table[K, V],
	key *K,
	hash uintptr,
	needValue bool,
) (v6Status, V, bool) {
	tag, start := v6HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, *new(V), false
		}
		if ctrl&v6WritingMask != 0 {
			return v6Retry, *new(V), false
		}
		words := v6LoadTagWords(b)
		match := v6MatchBits(words, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if e.key == *key {
				ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
				if status != v6OK {
					return status, *new(V), false
				}
				var prev V
				if needValue {
					prev = e.val
				}
				if !v6EnableSameKeyTombstoneReuse {
					slot.WriteUnfenced(v6Entry[K, V]{})
				} else {
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key})
				}
				ctrl = v6SetTag(ctrl, lane, v6TagDeleted)
				v6EndWriteModified(b, ctrl)
				m.size.Add(v6CntDeleted, 1)
				return v6OK, prev, true
			}
			match &= match - 1
		}
		if v6EmptyBits(words) != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			return v6OK, *new(V), false
		}
		if ctrl != b.state.Load() {
			goto retryBucket
		}
	}
	return v6Full, *new(V), false
}

func (m *V6Map[K, V]) compareAndSwapIn(
	table *v6Table[K, V],
	key *K,
	hash uintptr,
	old *V,
	new *V,
) (v6Status, bool) {
	tag, start := v6HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, false
		}
		if ctrl&v6WritingMask != 0 {
			return v6Retry, false
		}
		words := v6LoadTagWords(b)
		match := v6MatchBits(words, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
					return v6OK, false
				}
				if m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(new))) {
					return v6OK, true
				}
				ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
				if status != v6OK {
					return status, false
				}
				slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: *new})
				v6EndWriteModified(b, ctrl)
				return v6OK, true
			}
			match &= match - 1
		}
		if v6EmptyBits(words) != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			return v6OK, false
		}
		if ctrl != b.state.Load() {
			goto retryBucket
		}
	}
	return v6Full, false
}

func (m *V6Map[K, V]) compareAndDeleteIn(
	table *v6Table[K, V],
	key *K,
	hash uintptr,
	old *V,
) (v6Status, bool) {
	tag, start := v6HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, false
		}
		if ctrl&v6WritingMask != 0 {
			return v6Retry, false
		}
		words := v6LoadTagWords(b)
		match := v6MatchBits(words, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
					return v6OK, false
				}
				ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
				if status != v6OK {
					return status, false
				}
				if !v6EnableSameKeyTombstoneReuse {
					slot.WriteUnfenced(v6Entry[K, V]{})
				} else {
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key})
				}
				ctrl = v6SetTag(ctrl, lane, v6TagDeleted)
				v6EndWriteModified(b, ctrl)
				m.size.Add(v6CntDeleted, 1)
				return v6OK, true
			}
			match &= match - 1
		}
		if v6EmptyBits(words) != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			return v6OK, false
		}
		if ctrl != b.state.Load() {
			goto retryBucket
		}
	}
	return v6Full, false
}

func (m *V6Map[K, V]) computeIn(
	table *v6Table[K, V],
	key *K,
	hash uintptr,
	fn func(e *MapEntry[K, V]),
) (v6Status, V, bool, bool) {
	tag, start := v6HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, *new(V), false, false
		}
		if ctrl&v6WritingMask != 0 {
			return v6Retry, *new(V), false, false
		}
		words := v6LoadTagWords(b)
		match := v6MatchBits(words, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if e.key == *key {
				ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
				if status != v6OK {
					return status, *new(V), false, false
				}
				it := MapEntry[K, V]{
					entry:  entry_[K, V]{hash: hash, key: *key, value: e.val},
					loaded: true,
				}
				fn(noEscape(&it))
				switch it.op {
				case updateOp:
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: it.entry.value})
					v6EndWriteModified(b, ctrl)
					return v6OK, it.entry.value, true, false
				case deleteOp:
					if !v6EnableSameKeyTombstoneReuse {
						slot.WriteUnfenced(v6Entry[K, V]{})
					} else {
						slot.WriteUnfenced(v6Entry[K, V]{key: e.key})
					}
					ctrl = v6SetTag(ctrl, lane, v6TagDeleted)
					v6EndWriteModified(b, ctrl)
					m.size.Add(v6CntDeleted, 1)
					return v6OK, it.entry.value, true, false
				default:
					v6EndWriteUnchanged(b, ctrl)
					return v6OK, it.entry.value, true, false
				}
			}
			match &= match - 1
		}
		if v6EnableSameKeyTombstoneReuse {
			deleted := v6DeletedBits(words)
			for deleted != 0 {
				lane := v6FirstMarkedLane(deleted)
				slot := table.entry(bi, lane)
				e := slot.ReadUnfenced()
				if ctrl != b.state.Load() {
					goto retryBucket
				}
				if e.key == *key {
					ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
					if status != v6OK {
						return status, *new(V), false, false
					}
					it := MapEntry[K, V]{
						entry: entry_[K, V]{hash: hash, key: *key},
					}
					fn(noEscape(&it))
					if it.op != updateOp {
						v6EndWriteUnchanged(b, ctrl)
						return v6OK, *new(V), false, false
					}
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: it.entry.value})
					ctrl = v6SetTag(ctrl, lane, tag)
					v6EndWriteModified(b, ctrl)
					m.size.Add(v6CntDeleted, ^uintptr(0))
					return v6OK, it.entry.value, false, false
				}
				deleted &= deleted - 1
			}
		}
		if empty := v6EmptyBits(words); empty != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
			if status != v6OK {
				return status, *new(V), false, false
			}
			lane := v6FirstMarkedLane(empty)
			it := MapEntry[K, V]{
				entry: entry_[K, V]{hash: hash, key: *key},
			}
			fn(noEscape(&it))
			if it.op != updateOp {
				v6EndWriteUnchanged(b, ctrl)
				return v6OK, *new(V), false, false
			}
			table.entry(bi, lane).WriteUnfenced(v6Entry[K, V]{key: *key, val: it.entry.value})
			ctrl = v6SetTag(ctrl, lane, tag)
			v6EndWriteModified(b, ctrl)
			m.size.Add(v6CntUsed, 1)
			return v6OK, it.entry.value, false, empty&(empty-1) == 0
		}
		if ctrl != b.state.Load() {
			goto retryBucket
		}
	}
	return v6Full, *new(V), false, false
}

func (m *V6Map[K, V]) resizeIfNeeded(table *v6Table[K, V]) {
	used := int(m.size.Value(v6CntUsed))
	if used >= table.growCap {
		m.tryResize(table, used, v6ResizeNormal)
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
	// deleted := int(m.size.Value(v6CntDeleted))
	// live := used - deleted
	// if deleted > v6SlotsPerBucket && deleted > live/4 {
	// 	m.tryResize(table, used, v6ResizeCompact)
	// }
}

func (m *V6Map[K, V]) tryResize(old *v6Table[K, V], used int, hint v6ResizeHint) *v6Table[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}
	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			deleted := int(m.size.Value(v6CntDeleted))
			live := used - deleted
			nextLen := v6CalcBucketLen(live)
			nextLen = max(nextLen, m.minLen)
			aggressive := hint == v6ResizeProbeLimit
			if v6EnableAggressiveGrow {
				curUsed := int(m.size.Value(v6CntUsed))
				usedInResize := curUsed - used
				aggressive = aggressive || usedInResize >= 2
			}
			if aggressive {
				nextLen = min(nextLen<<1, old.bucketLen()<<2)
			}
			next = newV6Table[K, V](nextLen, old.intKey)
			old.nextTable.Store(next)
		} else {
			if v6EnableStoreInGrow {
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

func (m *V6Map[K, V]) helpResize(old *v6Table[K, V]) *v6Table[K, V] {
	next := old.nextTable.Load()
	if next == nil {
		return m.tryResize(old, int(m.size.Value(v6CntUsed)), v6ResizeProbeLimit)
	}
	return m.helpResizeInto(old, next)
}

func (m *V6Map[K, V]) helpResizeInto(old, next *v6Table[K, V]) *v6Table[K, V] {
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
			words := v6FreezeAndLoadTags(b)
			full := v6FullBits(words)
			for full != 0 {
				lane := v6FirstMarkedLane(full)
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
			observed := next.copyMaxProbe.Load() + 1
			next.probeLimit = min(next.bucketLen(), nextPowOf2(max(observed<<1, uintptr(v6MaxProbeBuckets))))
			used := m.size.Reset(v6CntUsed)
			deleted := m.size.Reset(v6CntDeleted)
			m.size.Add(v6CntUsed, used-deleted)
			m.table.CompareAndSwap(old, next)
		}
	}
}

func (m *V6Map[K, V]) ensureTable() *v6Table[K, V] {
	if table := m.table.Load(); table != nil {
		return table
	}
	return m.slowInit()
}

//go:noinline
func (m *V6Map[K, V]) slowInit() *v6Table[K, V] {
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
func (m *V6Map[K, V]) hashKey(key *K) uintptr {
	if m.intKey {
		return intHash[K](noescape(unsafe.Pointer(key)))
	}
	return m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
}

func newV6Table[K comparable, V any](bucketLen uintptr, intKey bool) *v6Table[K, V] {
	bucketLen = nextPowOf2(max(bucketLen, uintptr(v6MinBuckets)))
	slotLen := bucketLen * v6SlotsPerBucket
	growCap := int(slotLen * v6LoadFactorNum / v6LoadFactorDen)
	cpus := max(uintptr(runtime.GOMAXPROCS(0)), 1)
	roundedSizeLen := nextPowOf2(cpus)
	stripeCap := int(uintptr(growCap) >> bits.TrailingZeros(uint(roundedSizeLen)))
	chunks, chunkSz := v6ResizeChunks(bucketLen, cpus)
	buckets := makeUnsafeSlice[v6Bucket](bucketLen)
	entries := makeUnsafeSlice[SeqLockSlot[v6Entry[K, V]]](slotLen)
	return &v6Table[K, V]{
		buckets:    buckets,
		entries:    entries,
		mask:       bucketLen - 1,
		probeLimit: min(bucketLen, uintptr(v6MaxProbeBuckets)),
		stripeCap:  stripeCap,
		growCap:    growCap,
		intKey:     intKey,
		chunks:     chunks,
		chunkSz:    chunkSz,
	}
}

//go:nosplit
func v6HashParts(hash uintptr, intKey bool, mask uintptr) (uint8, uintptr) {
	// V6 uses h2-format tags: the high bit marks a full lane and the lower
	// seven bits carry entropy. The lost tag bit buys a cheaper SWAR full-lane
	// test: v6FullBits can be just a high-bit mask.
	if intKey {
		mixed := uint64(hash) * uint64(0x9e3779b97f4a7c15)
		tag := h2(uintptr(mixed >> 56))
		return tag, uintptr(mixed>>32) & mask
	}
	return h2(hash), h1(hash) & mask
}

//go:nosplit
func v6CalcBucketLen(capacity int) uintptr {
	if capacity <= 0 {
		return v6MinBuckets
	}
	needSlots := uintptr(capacity+1) * v6LoadFactorDen / v6LoadFactorNum
	needBuckets := (needSlots + v6SlotsPerBucket - 1) / v6SlotsPerBucket
	return nextPowOf2(max(needBuckets, uintptr(v6MinBuckets)))
}

//go:nosplit
func v6ResizeChunks(bucketLen, cpus uintptr) (chunks uint32, chunkSz uintptr) {
	const overCpus = resizeOverPartition
	want := min(bucketLen/v6MinBuckets, max(cpus*overCpus, 1))
	if want <= 1 {
		return 1, bucketLen
	}
	c := uint32(1) << (bits.Len32(uint32(want)) - 1)
	return c, bucketLen >> bits.TrailingZeros64(uint64(c))
}

//go:nosplit
func (table *v6Table[K, V]) bucketLen() uintptr {
	return table.mask + 1
}

//go:nosplit
func (table *v6Table[K, V]) entry(bucketIdx, lane uintptr) *SeqLockSlot[v6Entry[K, V]] {
	return table.entries.At(bucketIdx*v6SlotsPerBucket + lane)
}

func (table *v6Table[K, V]) copyInsertConcurrent(e v6Entry[K, V], hash uintptr) uintptr {
	tag, start := v6HashParts(hash, table.intKey, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
		ctrl, status := v6BeginWrite(b)
		if status == v6Retry {
			probe--
			runtime.Gosched()
			continue
		}
		if status != v6OK {
			probe--
			runtime.Gosched()
			continue
		}
		words := v6LoadTagWords(b)
		empty := v6EmptyBits(words)
		if empty == 0 {
			v6EndWriteUnchanged(b, ctrl)
			continue
		}
		lane := v6FirstMarkedLane(empty)
		dst := table.entry(bi, lane)
		dst.WriteUnfenced(e)
		ctrl = v6SetTag(ctrl, lane, tag)
		v6EndWriteModified(b, ctrl)
		return probe
	}
	panic("cc: V6Map grow produced a full table")
}

func v6BeginWrite(b *v6Bucket) (uint64, v6Status) {
	ctrl := b.state.Load()
	return v6BeginWriteWithCtrl(b, ctrl)
}

func v6BeginWriteWithCtrl(b *v6Bucket, ctrl uint64) (uint64, v6Status) {
	if ctrl&v6FrozenMask != 0 {
		return 0, v6Frozen
	}
	if ctrl&v6WritingMask != 0 {
		return 0, v6Retry
	}
	if !b.state.CompareAndSwap(ctrl, ctrl|v6WritingMask) {
		return 0, v6Retry
	}
	return ctrl, v6OK
}

func v6EndWriteUnchanged(b *v6Bucket, ctrl uint64) {
	b.state.Store(ctrl &^ v6WritingMask)
}

func v6EndWriteModified(b *v6Bucket, ctrl uint64) {
	b.state.Store(v6BumpCtrl(ctrl))
}

func v6FreezeAndLoadTags(b *v6Bucket) uint64 {
	for {
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6LoadTagWords(b)
		}
		if ctrl&v6WritingMask != 0 {
			runtime.Gosched()
			continue
		}
		if b.state.CompareAndSwap(ctrl, ctrl|v6WritingMask) {
			b.state.Store(v6BumpCtrl(ctrl | v6FrozenMask | v6WritingMask))
			return v6LoadTagWords(b)
		}
	}
}

//go:nosplit
func v6BumpCtrl(ctrl uint64) uint64 {
	return (ctrl &^ v6WritingMask) + v6VersionInc
}

//go:nosplit
func v6LoadTagWords(b *v6Bucket) uint64 {
	return b.state.Load()
}

//go:nosplit
func v6SetTag(state uint64, lane uintptr, tag uint8) uint64 {
	shift := lane << 3
	mask := uint64(0xff) << shift
	return (state &^ mask) | uint64(tag)<<shift
}

//go:nosplit
func v6MatchBits(words uint64, tag uint8) uint64 {
	// The match path verifies candidate lanes with the full key after taking a
	// ctrl snapshot, so the fast SWAR zero check may return harmless false
	// positives. Empty/deleted checks below need the exact variant because they
	// control probe termination and tombstone reuse.
	return v6MaybeZeroByteBits(words ^ v6BroadcastTag(tag))
}

//go:nosplit
func v6EmptyBits(words uint64) uint64 {
	return v6ZeroByteBits(words)
}

//go:nosplit
func v6DeletedBits(words uint64) uint64 {
	return v6ZeroByteBits(words ^ v6BroadcastTag(v6TagDeleted))
}

//go:nosplit
func v6FullBits(words uint64) uint64 {
	// Full tags use the h2 byte format and always carry the high bit. Empty (0)
	// and deleted (1) never do, so a high-bit mask is enough here.
	return words & v6LaneMarkerMask
}

//go:nosplit
func v6BroadcastTag(tag uint8) uint64 {
	return uint64(tag) * 0x010101010101
}

//go:nosplit
func v6MaybeZeroByteBits(words uint64) uint64 {
	const lowBits = uint64(0x010101010101)
	return (words - lowBits) &^ words & v6LaneMarkerMask
}

//go:nosplit
func v6ZeroByteBits(words uint64) uint64 {
	const lowBits = uint64(0x7f7f7f7f7f7f)
	return ^(((words & lowBits) + lowBits) | words | lowBits) & v6LaneMarkerMask
}

//go:nosplit
func v6FirstMarkedLane(marked uint64) uintptr {
	return uintptr(bits.TrailingZeros64(marked)) >> 3
}
