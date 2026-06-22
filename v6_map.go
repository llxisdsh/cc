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
	// v6EnableTerminalTombstoneReuse lets an insert reuse a deleted lane in the
	// same bucket after an empty lane proves the key absent.
	v6EnableTerminalTombstoneReuse = true
)

const (
	v6MinBuckets     = 32
	v6SlotsPerBucket = 6
	v6LoadFactor     = 0.75
	v6LaneMarkerMask = uint64(0x808080808080)
)

const (
	v6TagEmpty   = uint8(0)
	v6TagDeleted = uint8(1)
)

const (
	// Keep ctrl for version/frozen/writing only. If miss-heavy workloads need a
	// probe-continuation hint, prefer a side structure so displaced inserts do not
	// have to update shared bucket metadata.
	v6FrozenMask  = uint64(1) << 49
	v6WritingMask = uint64(1) << 48
	v6VersionMask = /* unused but kept for ref */ uint64(0x3fff) << 50
	v6VersionInc  = uint64(1) << 50
)

const (
	// occupied tracks physical lanes that are no longer Empty: Full + Tombstone.
	// tombstones tracks lanes currently in Deleted/Tombstone state, not total
	// delete operations.
	v6CntOccupied = iota
	v6CntTombstones
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
// Each bucket stores six one-byte tags plus a compact control word. Entries
// are kept in a separate flat array and addressed by bucket/lane, so probing
// stays compact while key/value storage remains contiguous. Reads use a bucket
// version snapshot; writes publish through a short per-bucket writing window,
// and resize freezes old buckets cooperatively. Like [OFHTMap], entry payloads
// are copied through SeqLockSlot so weak-memory architectures do not move
// large key/value reads outside the version-checked window.
//
// Integer keys use the fast integer hash path. Other comparable key shapes
// keep Go's built-in hasher to preserve == semantics. Use [WithKeyHasherUnsafe]
// to supply a custom hasher for custom key types.
type V6Map[K comparable, V any] struct {
	_         noCopy
	table     atomic.Pointer[v6Table[K, V]]
	rs        atomic.Pointer[v6RebuildState]
	initState atomic.Uint32
	intKey    bool
	seed      uintptr
	keyHash   HashFunc
	valEqual  EqualFunc
	minLen    uintptr
}

// v6RebuildState blocks new writers while a Rebuild (or Clear) holds the map.
// Readers never wait on it; cooperative resize is independent of it.
type v6RebuildState struct {
	latch Latch
}

type v6Table[K comparable, V any] struct {
	buckets      unsafeSlice[v6Bucket]
	entries      unsafeSlice[SeqLockSlot[v6Entry[K, V]]]
	mask         uintptr
	probeLimit   uintptr
	stripeCap    int
	growCap      int
	size         PooledLocalCounterN
	chunkSz      uintptr
	chunks       uint32
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
// ABA boundary: the single atomic state makes tag and control snapshots
// tear-free. The 14-bit version is not a formal ABA-proof sequence counter, but
// a harmful ABA requires all of these conditions at once:
//  1. writes to the same bucket complete a full 16,384-version wrap;
//  2. a reader keeps an unfenced entry copy from the old snapshot across that
//     wrap; and
//  3. the repeated state validates a torn K/V value from the entry copy.
//
// For normal small-K/V workloads this is a strong practical boundary: the entry
// copy is usually only a few machine-word loads, while the version wrap requires
// sustained concurrent writes to the same bucket. A public-API stress test using
// string keys and string values ran for 30 minutes without reproducing a
// failure. The remaining risk is concentrated in larger K/V types, extremely hot
// buckets, long scheduler or OS pauses, and very high-frequency mutation of the
// same bucket. V6 intentionally favors compact buckets and read-path speed over
// formal ABA immunity for arbitrary K/V sizes.
//
// ┌───────────────────┬──────┬───────┬─────────────────────────────────────┐
// │ version (14 bits) │frozen│writing│     6 × 8-bit h2 tags (48 bits)     │
// │    bits 63-50     │bit 49│bit 48 │              bits 47-0              │
// └───────────────────┴──────┴───────┴─────────────────────────────────────┘
type v6Bucket struct {
	state atomic.Uint64
}

// v6Entry intentionally does not cache the full hash.
//
// An entryWithHash-style layout was tested for string keys. It did not produce
// a stable insert-throughput win in V6: bucket tags already filter most failed
// candidates before key equality, and adding a hash widens every flat entry,
// increasing cache footprint and resize copy traffic. Keep the hash out of the
// V6 payload unless a future benchmark demonstrates a clear net win for a
// specific larger-key workload.
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
	m.table.Store(newV6Table[K, V](m.minLen))
}

func (m *V6Map[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	var hash uintptr
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(&key)))
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
	}
	tag, start := v6HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
		// spins := 0
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6WritingMask != 0 {
			// delay(&spins)
			goto retryBucket
		}
		match := v6MatchBits(ctrl, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			e := table.entry(bi, lane).ReadUnfenced()
			ctrl2 := b.state.Load()
			if ctrl != ctrl2 {
				goto retryBucket
			}
			if e.key == key {
				return e.val, true
			}
			match &= match - 1
		}
		if v6EmptyBits(ctrl) != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			return *new(V), false
		}
	}
	return *new(V), false
}

func (m *V6Map[K, V]) Store(key K, value V) {
	m.store(&key, &value, false)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *V6Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.store(&key, &value, true)
}

// LoadOrStoreFn loads the value for a key if present.
// Otherwise, it stores and returns the value returned by valueFn.
// The loaded result is true if the value was loaded, false if stored.
// valueFn is only invoked when the key is absent.
func (m *V6Map[K, V]) LoadOrStoreFn(
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

// Swap stores value for key and returns the previous value if any.
// The loaded result reports whether the key was present.
func (m *V6Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	actual, loaded := m.store(&key, &value, false)
	if !loaded {
		return *new(V), false
	}
	return actual, true
}

func (m *V6Map[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	return m.update(&key, &value, false)
}

func (m *V6Map[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	return m.delete(&key, true)
}

func (m *V6Map[K, V]) Delete(key K) {
	m.delete(&key, false)
}

func (m *V6Map[K, V]) CompareAndSwap(key K, old V, new V) (swapped bool) {
	if m.table.Load() == nil {
		return false
	}
	if m.valEqual == nil {
		panicV6ValueNotComparable()
	}
	fn := func(e *MapEntry[K, V]) {
		if e.Loaded() {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.entry.val)),
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

func (m *V6Map[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	if m.table.Load() == nil {
		return false
	}
	if m.valEqual == nil {
		panicV6ValueNotComparable()
	}
	fn := func(e *MapEntry[K, V]) {
		if e.Loaded() {
			if m.valEqual(
				noescape(unsafe.Pointer(&e.entry.val)),
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
// Callback signature:
//
//		fn(e *MapEntry[K, V])
//
//	  - Use e.Loaded() and e.Value() to inspect the current state
//	  - Use e.Update(newV) to upsert; Use e.Delete() to remove
//
// The callback runs inside the bucket's short write window; keep it
// lightweight and do not call other operations on the same map from it.
//
// Returns:
//   - actual: the current value in the map after the operation
//   - loaded: true if the key existed before the operation
func (m *V6Map[K, V]) Compute(key K, fn func(e *MapEntry[K, V])) (actual V, loaded bool) {
	return m.compute(&key, unsafe.Pointer(&fn), computeInit)
}

// compute adapts Map.compute's shape so MapRebuild can dispatch to either
// implementation under both build tags. val must be a *func(e *MapEntry[K, V]).
func (m *V6Map[K, V]) compute(
	key *K,
	val unsafe.Pointer, // *func(e *MapEntry[K, V])
	flags computeFlags,
) (actual V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		if flags&computeInit == 0 {
			return *new(V), false
		}
		table = m.slowInit()
	}
	var hash uintptr
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(key)))
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
	}
	fn := *(*func(e *MapEntry[K, V]))(val)
	for {
		if flags&computeIgnoreHint == 0 {
			if rs := m.rs.Load(); rs != nil {
				rs.latch.Wait()
				table = m.table.Load()
				continue
			}
		}
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.computeIn(table, key, hash, fn, flags)
		switch status {
		case v6OK:
			if shouldCheckResize {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v6Full:
			table = m.tryResize(table, int(table.size.Value(v6CntOccupied)), v6ResizeProbeLimit)
		case v6Frozen:
			table = m.helpResize(table)
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
		full := v6FullBits(ctrl)
		var cacheCount uintptr
		for full != 0 {
			lane := v6FirstMarkedLane(full)
			*unsafeCache.At(cacheCount) = table.entry(i, lane).ReadUnfenced()
			cacheCount++
			full &= full - 1
		}
		ctrl2 := b.state.Load()
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

func (m *V6Map[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// ToMap collects up to limit entries into a map[K]V.
// Omitting limit collects everything; limit <= 0 returns an empty map.
func (m *V6Map[K, V]) ToMap(limit ...int) map[K]V {
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
func (m *V6Map[K, V]) Entries() func(yield func(e *MapEntry[K, V]) bool) {
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
//   - The callback runs inside the bucket's write window: concurrent readers
//     and writers of that bucket spin until the window closes. Keep yield
//     very lightweight, and never call operations on the same map from it
//     (doing so deadlocks).
//   - Cooperates with concurrent grow/shrink; if a resize is detected, it
//     helps complete copying, then restarts on the latest table.
//   - Provides weakly consistent iteration: entries may be updated, added,
//     or removed concurrently, may or may not be observed, and may be
//     observed more than once after an iteration restart.
func (m *V6Map[K, V]) ComputeRange(yield func(e *MapEntry[K, V]) bool) {
	m.computeRange(yield, false)
}

func (m *V6Map[K, V]) computeRange(yield func(e *MapEntry[K, V]) bool, ignoreRebuildState bool) {
restart:
	table := m.table.Load()
	if table == nil {
		return
	}
	it := MapEntry[K, V]{
		loaded: true,
	}
	for i := uintptr(0); i <= table.mask; i++ {
		if !ignoreRebuildState {
			if rs := m.rs.Load(); rs != nil {
				rs.latch.Wait()
				goto restart
			}
			if newTable := m.table.Load(); table != newTable {
				goto restart
			}
		}
		b := table.buckets.At(i)
	retryBucket:
		ctrl, status := v6BeginWrite(b)
		if status != v6OK {
			if status == v6Retry {
				goto retryBucket
			}
			// v6Frozen: help finish the resize, restart on the latest table.
			m.helpResize(table)
			goto restart
		}
		newCtrl := ctrl
		tombstoneAdds := uintptr(0)
		modified := false
		stop := false
		for full := v6FullBits(ctrl); full != 0; full &= full - 1 {
			lane := v6FirstMarkedLane(full)
			slot := table.entry(i, lane)
			e := slot.ReadUnfenced()
			it.entry.key, it.entry.val = e.key, e.val
			it.op = cancelOp
			shouldContinue := yield(noEscape(&it))
			switch it.op {
			case updateOp:
				slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: it.entry.val})
				modified = true
			case deleteOp:
				if !v6EnableSameKeyTombstoneReuse {
					slot.WriteUnfenced(v6Entry[K, V]{})
				} else {
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key})
				}
				newCtrl = v6SetTag(newCtrl, lane, v6TagDeleted)
				tombstoneAdds++
				modified = true
			default:
				// cancelOp: no-op
			}
			if !shouldContinue {
				stop = true
				break
			}
		}
		if modified {
			v6EndWriteModified(b, newCtrl)
		} else {
			v6EndWriteUnchanged(b, newCtrl)
		}
		if tombstoneAdds != 0 {
			table.size.Add(v6CntTombstones, tombstoneAdds)
		}
		if stop {
			return
		}
	}
}

type v6MapStats struct {
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

func (m *V6Map[K, V]) stats() v6MapStats {
	table := m.table.Load()
	var occupied, tombstones uintptr
	if table != nil {
		occupied = table.size.Value(v6CntOccupied)
		tombstones = table.size.Value(v6CntTombstones)
	}
	stats := v6MapStats{
		Live:       occupied - tombstones,
		Occupied:   occupied,
		Tombstones: tombstones,
	}
	if table == nil {
		return stats
	}
	stats.Buckets = table.bucketLen()
	stats.Capacity = stats.Buckets * v6SlotsPerBucket
	for i := uintptr(0); i <= table.mask; i++ {
		b := table.buckets.At(i)
		ctrl := b.state.Load()
		full := v6FullBits(ctrl)
		empty := v6EmptyBits(ctrl)
		tombstones := v6DeletedBits(ctrl)
		fullCount := uintptr(bits.OnesCount64(full))
		stats.FullLanes += fullCount
		stats.EmptyLanes += uintptr(bits.OnesCount64(empty))
		stats.TombstoneLanes += uintptr(bits.OnesCount64(tombstones))
		if fullCount == v6SlotsPerBucket {
			stats.FullBuckets++
		}
		fullScan := full
		for fullScan != 0 {
			lane := v6FirstMarkedLane(fullScan)
			e := table.entry(i, lane).ReadUnfenced()
			var hash uintptr
			if m.intKey {
				hash = intHash[K](noescape(unsafe.Pointer(&e.key)))
			} else {
				hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
			}
			_, start := v6HashParts(hash, m.intKey, table.mask)
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
	table := m.table.Load()
	if table == nil {
		return 0
	}
	occupied := int(table.size.Value(v6CntOccupied))
	tombstones := int(table.size.Value(v6CntTombstones))
	return max(occupied-tombstones, 0)
}

// Clear clears all key-value pairs from the map.
// New writers wait while the table is replaced. Writers that already entered
// before the rebuild state was installed may finish on their captured table.
func (m *V6Map[K, V]) Clear() {
	if m.table.Load() == nil {
		return
	}
	for {
		if rs := m.rs.Load(); rs != nil {
			rs.latch.Wait()
			continue
		}
		rs := m.beginRebuild()
		if rs == nil {
			continue
		}
		m.drainResize()
		m.table.Store(newV6Table[K, V](m.minLen))
		m.endRebuild(rs)
		return
	}
}

// Rebuild performs a map rebuild operation with the given function.
// New public writers wait while the rebuild state is active; concurrent readers
// are allowed. Writers that already entered before the state was installed may
// finish on their captured table.
//
// Parameters:
//   - fn: The function to execute during rebuild.
//     It receives a MapRebuild instance.
//
// Notes:
//   - You must use the `m *MapRebuild[K, V]` parameter passed to `fn` for
//     processing. Do not call methods on the Map instance directly, as this
//     may cause deadlocks.
func (m *V6Map[K, V]) Rebuild(fn func(m *MapRebuild[K, V])) {
	for {
		if rs := m.rs.Load(); rs != nil {
			rs.latch.Wait()
			continue
		}
		rs := m.beginRebuild()
		if rs == nil {
			continue
		}
		m.drainResize()
		fn(noEscape(&MapRebuild[K, V]{f: m}))
		m.endRebuild(rs)
		return
	}
}

func (m *V6Map[K, V]) beginRebuild() *v6RebuildState {
	if m.rs.Load() != nil {
		return nil
	}
	rs := &v6RebuildState{}
	if !m.rs.CompareAndSwap(nil, rs) {
		return nil
	}
	return rs
}

func (m *V6Map[K, V]) endRebuild(rs *v6RebuildState) {
	m.rs.Store(nil)
	rs.latch.Open()
}

// drainResize completes any in-flight cooperative resize so the rebuild
// holder starts from a stable table. Must be called after beginRebuild.
// It must not wait on m.rs (self-deadlock).
func (m *V6Map[K, V]) drainResize() {
	for {
		table := m.table.Load()
		if table == nil {
			return
		}
		if next := table.nextTable.Load(); next != nil {
			m.helpResizeInto(table, next)
			continue
		}
		if table.allocating.Load() == 0 {
			return
		}
		runtime.Gosched()
	}
}

func (m *V6Map[K, V]) store(key *K, val *V, onlyIfAbsent bool) (actual V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		table = m.slowInit()
	}
	var hash uintptr
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(key)))
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
	}
	for {
		if rs := m.rs.Load(); rs != nil {
			rs.latch.Wait()
			table = m.table.Load()
			continue
		}
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			continue
		}
		status, actual, loaded, shouldCheckResize := m.storeIn(table, key, val, hash, onlyIfAbsent)
		switch status {
		case v6OK:
			if shouldCheckResize {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case v6Full:
			table = m.tryResize(table, int(table.size.Value(v6CntOccupied)), v6ResizeProbeLimit)
		case v6Frozen:
			table = m.helpResize(table)
		}
	}
}

func (m *V6Map[K, V]) update(key *K, val *V, onlyIfAbsent bool) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	var hash uintptr
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(key)))
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
	}
	for {
		if rs := m.rs.Load(); rs != nil {
			rs.latch.Wait()
			table = m.table.Load()
			if table == nil {
				return *new(V), false
			}
			continue
		}
		if next := table.nextTable.Load(); next != nil {
			table = m.helpResizeInto(table, next)
			if table == nil {
				return *new(V), false
			}
			continue
		}
		status, previous, loaded := m.updateIn(table, key, val, hash, onlyIfAbsent)
		switch status {
		case v6OK:
			return previous, loaded
		case v6Full:
			return *new(V), false
		case v6Frozen:
			table = m.helpResize(table)
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
	tag, start := v6HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, *new(V), false, false
		}
		if ctrl&v6WritingMask != 0 {
			goto retryBucket
		}
		match := v6MatchBits(ctrl, tag)
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
					if status == v6Retry {
						goto retryBucket
					}
					return status, *new(V), false, false
				}
				slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: *val})
				v6EndWriteModified(b, ctrl)
				// e is still current: the CAS in v6BeginWriteWithCtrl validated
				// the same ctrl snapshot the read was checked against.
				return v6OK, e.val, true, false
			}
			match &= match - 1
		}
		if empty := v6EmptyBits(ctrl); empty != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
			if status != v6OK {
				if status == v6Retry {
					goto retryBucket
				}
				return status, *new(V), false, false
			}
			lane, reuseDeleted := v6InsertLane(ctrl, empty)
			table.entry(bi, lane).WriteUnfenced(v6Entry[K, V]{key: *key, val: *val})
			ctrl = v6SetTag(ctrl, lane, tag)
			v6EndWriteModified(b, ctrl)
			if reuseDeleted {
				table.size.Add(v6CntTombstones, ^uintptr(0))
				return v6OK, *val, false, false
			}
			local := table.size.Add(v6CntOccupied, 1)
			return v6OK, *val, false, empty&(empty-1) == 0 && int(local) >= table.stripeCap
		}
		if v6EnableSameKeyTombstoneReuse {
			tombstones := v6DeletedBits(ctrl)
			for tombstones != 0 {
				lane := v6FirstMarkedLane(tombstones)
				slot := table.entry(bi, lane)
				e := slot.ReadUnfenced()
				if ctrl != b.state.Load() {
					goto retryBucket
				}
				if e.key == *key {
					ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
					if status != v6OK {
						if status == v6Retry {
							goto retryBucket
						}
						return status, *new(V), false, false
					}
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: *val})
					ctrl = v6SetTag(ctrl, lane, tag)
					v6EndWriteModified(b, ctrl)
					table.size.Add(v6CntTombstones, ^uintptr(0))
					return v6OK, *val, false, false
				}
				tombstones &= tombstones - 1
			}
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
) (v6Status, V, bool) {
	tag, start := v6HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, *new(V), false
		}
		if ctrl&v6WritingMask != 0 {
			goto retryBucket
		}
		match := v6MatchBits(ctrl, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if onlyIfAbsent {
					return v6OK, e.val, true
				}
				if v6EnableDedupVal && m.valEqual != nil &&
					m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return v6OK, e.val, true
				}
				ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
				if status != v6OK {
					if status == v6Retry {
						goto retryBucket
					}
					return status, *new(V), false
				}
				slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: *val})
				v6EndWriteModified(b, ctrl)
				return v6OK, e.val, true
			}
			match &= match - 1
		}
		if v6EmptyBits(ctrl) != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			return v6OK, *new(V), false
		}
		if ctrl != b.state.Load() {
			goto retryBucket
		}
	}
	return v6OK, *new(V), false
}

func (m *V6Map[K, V]) delete(key *K, needValue bool) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	var hash uintptr
	if m.intKey {
		hash = intHash[K](noescape(unsafe.Pointer(key)))
	} else {
		hash = m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
	}
	for {
		if rs := m.rs.Load(); rs != nil {
			rs.latch.Wait()
			table = m.table.Load()
			if table == nil {
				return *new(V), false
			}
			continue
		}
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
		}
	}
}

func (m *V6Map[K, V]) deleteIn(
	table *v6Table[K, V],
	key *K,
	hash uintptr,
	needValue bool,
) (v6Status, V, bool) {
	tag, start := v6HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)

	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, *new(V), false
		}
		if ctrl&v6WritingMask != 0 {
			goto retryBucket
		}
		match := v6MatchBits(ctrl, tag)
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
					if status == v6Retry {
						goto retryBucket
					}
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
				table.size.Add(v6CntTombstones, 1)
				return v6OK, prev, true
			}
			match &= match - 1
		}
		if v6EmptyBits(ctrl) != 0 {
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

func (m *V6Map[K, V]) computeIn(
	table *v6Table[K, V],
	key *K,
	hash uintptr,
	fn func(e *MapEntry[K, V]),
	flags computeFlags,
) (v6Status, V, bool, bool) {
	tag, start := v6HashParts(hash, m.intKey, table.mask)
	for probe := uintptr(0); probe < table.probeLimit; probe++ {
		bi := (start + probe) & table.mask
		b := table.buckets.At(bi)
	retryBucket:
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return v6Frozen, *new(V), false, false
		}
		if ctrl&v6WritingMask != 0 {
			goto retryBucket
		}
		match := v6MatchBits(ctrl, tag)
		for match != 0 {
			lane := v6FirstMarkedLane(match)
			slot := table.entry(bi, lane)
			e := slot.ReadUnfenced()
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if e.key == *key {
				if flags&computeSkipIfFound != 0 {
					return v6OK, e.val, true, false
				}
				ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
				if status != v6OK {
					if status == v6Retry {
						goto retryBucket
					}
					return status, *new(V), false, false
				}
				it := MapEntry[K, V]{
					entry:  entryNoHash[K, V]{key: *key, val: e.val},
					loaded: true,
				}
				fn(noEscape(&it))
				ret := it.entry.val
				switch it.op {
				case updateOp:
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: it.entry.val})
					v6EndWriteModified(b, ctrl)
					return v6OK, ret, true, false
				case deleteOp:
					if !v6EnableSameKeyTombstoneReuse {
						slot.WriteUnfenced(v6Entry[K, V]{})
					} else {
						slot.WriteUnfenced(v6Entry[K, V]{key: e.key})
					}
					ctrl = v6SetTag(ctrl, lane, v6TagDeleted)
					v6EndWriteModified(b, ctrl)
					table.size.Add(v6CntTombstones, 1)
					return v6OK, ret, true, false
				default:
					v6EndWriteUnchanged(b, ctrl)
					return v6OK, ret, true, false
				}
			}
			match &= match - 1
		}
		if empty := v6EmptyBits(ctrl); empty != 0 {
			if ctrl != b.state.Load() {
				goto retryBucket
			}
			if flags&computeSkipIfNotFound != 0 {
				return v6OK, *new(V), false, false
			}
			ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
			if status != v6OK {
				if status == v6Retry {
					goto retryBucket
				}
				return status, *new(V), false, false
			}
			lane, reuseDeleted := v6InsertLane(ctrl, empty)
			it := MapEntry[K, V]{
				entry: entryNoHash[K, V]{key: *key},
			}
			fn(noEscape(&it))
			if it.op != updateOp {
				v6EndWriteUnchanged(b, ctrl)
				return v6OK, *new(V), false, false
			}
			table.entry(bi, lane).WriteUnfenced(v6Entry[K, V]{key: *key, val: it.entry.val})
			ctrl = v6SetTag(ctrl, lane, tag)
			v6EndWriteModified(b, ctrl)
			if reuseDeleted {
				table.size.Add(v6CntTombstones, ^uintptr(0))
				return v6OK, it.entry.val, false, false
			}
			local := table.size.Add(v6CntOccupied, 1)
			return v6OK, it.entry.val, false, empty&(empty-1) == 0 && int(local) >= table.stripeCap
		}
		if v6EnableSameKeyTombstoneReuse {
			tombstones := v6DeletedBits(ctrl)
			for tombstones != 0 {
				lane := v6FirstMarkedLane(tombstones)
				slot := table.entry(bi, lane)
				e := slot.ReadUnfenced()
				if ctrl != b.state.Load() {
					goto retryBucket
				}
				if e.key == *key {
					if flags&computeSkipIfNotFound != 0 {
						return v6OK, *new(V), false, false
					}
					ctrl, status := v6BeginWriteWithCtrl(b, ctrl)
					if status != v6OK {
						if status == v6Retry {
							goto retryBucket
						}
						return status, *new(V), false, false
					}
					it := MapEntry[K, V]{
						entry: entryNoHash[K, V]{key: *key},
					}
					fn(noEscape(&it))
					if it.op != updateOp {
						v6EndWriteUnchanged(b, ctrl)
						return v6OK, *new(V), false, false
					}
					slot.WriteUnfenced(v6Entry[K, V]{key: e.key, val: it.entry.val})
					ctrl = v6SetTag(ctrl, lane, tag)
					v6EndWriteModified(b, ctrl)
					table.size.Add(v6CntTombstones, ^uintptr(0))
					return v6OK, it.entry.val, false, false
				}
				tombstones &= tombstones - 1
			}
		}
		if ctrl != b.state.Load() {
			goto retryBucket
		}
	}
	return v6Full, *new(V), false, false
}

func (m *V6Map[K, V]) resizeIfNeeded(table *v6Table[K, V]) {
	if v6EnableStoreInGrow {
		if table.allocating.Load() != 0 {
			return
		}
	}
	occupied := int(table.size.Value(v6CntOccupied))
	if occupied >= table.growCap {
		m.tryResize(table, occupied, v6ResizeNormal)
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
	// tombstones := int(table.size.Value(v6CntTombstones))
	// live := occupied - tombstones
	// if tombstones > v6SlotsPerBucket && tombstones > live/4 {
	// 	m.tryResize(table, occupied, v6ResizeCompact)
	// }
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
//   - Sizing is based on live entries; tombstones still occupy physical slots
//     until the next compaction, so a tombstone-heavy map may resize earlier
//     than the pre-allocated capacity suggests.
func (m *V6Map[K, V]) Grow(sizeAdd int) {
	if sizeAdd <= 0 {
		return
	}
	table := m.table.Load()
	if table == nil {
		table = m.slowInit()
	}
	for {
		if rs := m.rs.Load(); rs != nil {
			rs.latch.Wait()
			table = m.table.Load()
			continue
		}
		occupied := int(table.size.Value(v6CntOccupied))
		tombstones := int(table.size.Value(v6CntTombstones))
		live := max(occupied-tombstones, 0)
		newLen := v6CalcBucketLen(live + sizeAdd)
		if newLen <= table.bucketLen() {
			return
		}
		table = m.tryResizeLen(table, newLen)
	}
}

// Shrink reduces the capacity to fit the current size.
// It never shrinks below the initial capacity (WithCapacity).
func (m *V6Map[K, V]) Shrink() {
	table := m.table.Load()
	if table == nil {
		return
	}
	for {
		if rs := m.rs.Load(); rs != nil {
			rs.latch.Wait()
			table = m.table.Load()
			if table == nil {
				return
			}
			continue
		}
		if table.bucketLen() <= m.minLen {
			return
		}
		occupied := int(table.size.Value(v6CntOccupied))
		tombstones := int(table.size.Value(v6CntTombstones))
		live := max(occupied-tombstones, 0)
		newLen := max(v6CalcBucketLen(live), m.minLen)
		if newLen >= table.bucketLen() {
			return
		}
		table = m.tryResizeLen(table, newLen)
	}
}

// tryResizeLen resizes to a caller-chosen bucket length. Unlike tryResize,
// the target length is supplied, but the winner still re-checks the fresh
// live count so a stale or too-tight target (e.g. Shrink racing inserts)
// cannot produce a table smaller than the current contents need.
func (m *V6Map[K, V]) tryResizeLen(old *v6Table[K, V], nextLen uintptr) *v6Table[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}
	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			occupied := int(old.size.Value(v6CntOccupied))
			tombstones := int(old.size.Value(v6CntTombstones))
			live := max(occupied-tombstones, 0)
			nextLen = max(nextLen, v6CalcBucketLen(live), m.minLen)
			next = newV6Table[K, V](nextLen)
			old.nextTable.Store(next)
		} else {
			for next == nil {
				runtime.Gosched()
				next = old.nextTable.Load()
			}
		}
	}
	return m.helpResizeInto(old, next)
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
func (m *V6Map[K, V]) CloneTo(clone *V6Map[K, V]) {
	clone.Clear()
	table := m.table.Load()
	if table == nil {
		return
	}
	clone.intKey = m.intKey
	clone.seed = m.seed
	clone.keyHash = m.keyHash
	clone.valEqual = m.valEqual
	clone.minLen = m.minLen
	occupied := int(table.size.Value(v6CntOccupied))
	tombstones := int(table.size.Value(v6CntTombstones))
	bucketLen := max(v6CalcBucketLen(max(occupied-tombstones, 0)), m.minLen)
	clone.table.Store(newV6Table[K, V](bucketLen))
	for k, v := range m.All() {
		clone.Store(k, v)
	}
}

func (m *V6Map[K, V]) tryResize(old *v6Table[K, V], occupied int, hint v6ResizeHint) *v6Table[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}
	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			// Base sizing follows the live entry count. At the normal grow
			// threshold this rounds to 2x; tombstone-heavy resize can stay at
			// the same size and compact tombstone slots away.
			tombstones := int(old.size.Value(v6CntTombstones))
			live := occupied - tombstones
			nextLen := v6CalcBucketLen(live)
			nextLen = max(nextLen, m.minLen)
			aggressive := hint == v6ResizeProbeLimit
			if v6EnableAggressiveGrow {
				curOccupied := int(old.size.Value(v6CntOccupied))
				occupiedInResize := curOccupied - occupied
				aggressive = aggressive || occupiedInResize >= 2
			}
			// Probe-limit resize or observed concurrent insert pressure gets
			// one extra size class, capped at 4x the old table.
			if aggressive {
				nextLen = min(nextLen<<1, old.bucketLen()<<2)
			}
			next = newV6Table[K, V](nextLen)
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
		return m.tryResize(old, int(old.size.Value(v6CntOccupied)), v6ResizeProbeLimit)
	}
	return m.helpResizeInto(old, next)
}

func (m *V6Map[K, V]) helpResizeInto(old, next *v6Table[K, V]) *v6Table[K, V] {
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
			words := v6FreezeAndLoadTags(b)
			full := v6FullBits(words)
			for full != 0 {
				lane := v6FirstMarkedLane(full)
				e := old.entry(i, lane).ReadUnfenced()
				var hash uintptr
				if m.intKey {
					hash = intHash[K](noescape(unsafe.Pointer(&e.key)))
				} else {
					hash = m.keyHash(noescape(unsafe.Pointer(&e.key)), m.seed)
				}
				tag, insertStart := v6HashParts(hash, m.intKey, next.mask)
				var probe uintptr
				for probe = 0; probe <= next.mask; probe++ {
					bi := (insertStart + probe) & next.mask
					b := next.buckets.At(bi)
					ctrl, status := v6BeginWrite(b)
					if status != v6OK {
						probe--
						continue
					}
					empty := v6EmptyBits(ctrl)
					if empty == 0 {
						v6EndWriteUnchanged(b, ctrl)
						continue
					}
					lane := v6FirstMarkedLane(empty)
					dst := next.entry(bi, lane)
					dst.WriteUnfenced(e)
					ctrl = v6SetTag(ctrl, lane, tag)
					v6EndWriteModified(b, ctrl)
					break
				}
				if probe > next.mask {
					panicV6GrowFullTable()
				}
				copied++
				if probe > copyMaxProbe {
					copyMaxProbe = probe
				}
				full &= full - 1
			}
		}
		if copied != 0 {
			next.size.Add(v6CntOccupied, copied)
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

func newV6Table[K comparable, V any](bucketLen uintptr) *v6Table[K, V] {
	bucketLen = nextPowOf2(max(bucketLen, uintptr(v6MinBuckets)))
	slotLen := bucketLen * v6SlotsPerBucket
	growCap := int(float64(slotLen) * v6LoadFactor)
	cpus := maxProcs()
	chunks, chunkSz := v6ResizeChunks(bucketLen, cpus)
	buckets := makeUnsafeSlice[v6Bucket](bucketLen)
	entries := makeUnsafeSlice[SeqLockSlot[v6Entry[K, V]]](slotLen)
	activeSizeSlots := cpus
	table := &v6Table[K, V]{
		buckets:    buckets,
		entries:    entries,
		mask:       bucketLen - 1,
		probeLimit: min(bucketLen, calcProbeLimit(bucketLen)),
		stripeCap:  (growCap + int(activeSizeSlots) - 1) / int(activeSizeSlots),
		growCap:    growCap,
		size:       NewPooledLocalCounterN(v6CntTombstones + 1),
		chunks:     chunks,
		chunkSz:    chunkSz,
	}
	return table
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
	const invLoadFactor = 1 / v6LoadFactor
	needSlots := uintptr(float64(capacity+1) * invLoadFactor)
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
	b.state.Store(ctrl)
}

func v6EndWriteModified(b *v6Bucket, ctrl uint64) {
	b.state.Store(v6BumpCtrl(ctrl))
}

func v6FreezeAndLoadTags(b *v6Bucket) uint64 {
	for {
		ctrl := b.state.Load()
		if ctrl&v6FrozenMask != 0 {
			return ctrl
		}
		if ctrl&v6WritingMask != 0 {
			continue
		}
		if b.state.CompareAndSwap(ctrl, ctrl|v6WritingMask) {
			b.state.Store(v6BumpCtrl(ctrl | v6FrozenMask))
			return ctrl
		}
	}
}

// v6BumpCtrl increments the version portion of the control word.
// Note: We do not need to clear v6WritingMask here because the returned
// ctrl from beginWrite does not have the writing mask set, and we just
// add v6VersionInc to it.
//
//go:nosplit
func v6BumpCtrl(ctrl uint64) uint64 {
	return ctrl + v6VersionInc
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

// v6InsertLane prefers a deleted lane in the same bucket snapshot when the
// bucket also has an empty lane, so absence is still proven by that snapshot.
// Reusing deleted lanes from earlier buckets would need a cross-bucket proof
// under concurrent delete/insert races.
//
//go:nosplit
func v6InsertLane(ctrl, empty uint64) (uintptr, bool) {
	if v6EnableTerminalTombstoneReuse {
		if deleted := v6DeletedBits(ctrl); deleted != 0 {
			return v6FirstMarkedLane(deleted), true
		}
	}
	return v6FirstMarkedLane(empty), false
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

//go:noinline
func panicV6ValueNotComparable() {
	panic("cc: value is not comparable; use WithValueEqual")
}

//go:noinline
func panicV6GrowFullTable() {
	panic("cc: V6Map grow produced a full table")
}
