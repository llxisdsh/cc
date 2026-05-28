//go:build !race && (amd64 || arm64)

package cc

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/llxisdsh/cc/internal/asm"
)

const (
	// dwhtEnableIntKey enables specialized fast path optimizations for integer keys.
	dwhtEnableIntKey = true
	// dwhtEnableDedupVal enables deduplication checks for identical values.
	dwhtEnableDedupVal = true
	// dwhtEnableAggressiveGrow doubles resize slack when table allocation observes
	// concurrent inserts after the resize trigger.
	dwhtEnableAggressiveGrow = true
	// dwhtEnableStoreInGrow lets writers continue inserting into the old table
	// while a resize leader is allocating/publishing the new table. Keep it off
	// when old-table writes or later migration work are expected to be expensive.
	dwhtEnableStoreInGrow = true
)

const (
	// dwhtMinSlots is the minimum number of slots in a table.
	dwhtMinSlots = 128
	// dwhtLoadFactor must be a multiple of 1/8, such as 0.5, 0.625, 0.75, 0.875, etc.
	dwhtLoadFactor = 0.625

	// dwhtMaxProbeThreshold is the baseline probe window (slots).
	//
	// Tail model (linear probing):
	//   P(any run >= k) <= n * alpha^k
	// => k*(n) = ceil( ln(n/epsilon) / -ln(alpha) ).
	//
	// The implementation rounds k* to power-of-two windows (nextPow2), so
	// thresholds become stepwise. With alpha=0.625 and epsilon=1e-6:
	//   k*(128) = 40 -> window 64,
	// and the 64->128 step appears when n > epsilon * alpha^(-64) (~1.1e7, ~2^24).
	//
	// We keep a fixed baseline floor for stable read/miss locality, and rely on
	// probe-limit-triggered growth to handle clustering or hash degeneration.
	dwhtMaxProbeThreshold = 64

	// dwhtGrowCheckMask is used as a bitwise AND mask to sample the local size counter.
	// This reduces the overhead of checking the global size on every insertion.
	// It MUST be strictly smaller than the initial grow threshold.
	// If this mask is too large, the table could fill up
	// or hit the probe threshold before sampling the global size in highly
	// concurrent cold starts.
	dwhtGrowCheckMask = dwhtMinSlots/8 - 1 // Checks every 8th local insert
)

// DWHTMap is an experimental double-word-CAS hash table.
//
// It uses open addressing with linear probing. Each table slot is two machine
// words: a control word and an entry pointer. The control word contains the
// slot state, a 32-bit hash fragment, a version counter, and a frozen bit
// used during resize.
//
// Concurrency model:
//   - Loads read the control word and entry pointer directly from the current
//     table. A full slot points to an immutable key/value entry.
//   - Writes publish slot changes with DWCAS, atomically replacing both the
//     control word and entry pointer. Updates allocate a new immutable entry
//     and swap the slot to point at it.
//   - Resize allocates a next table, cooperatively freezes old slots, copies
//     frozen full slots by reusing their entry pointers, waits for all resize
//     chunks to finish, then publishes the new table.
//
// Compared with [OFHTMap], DWHTMap pays one heap object per live entry, but slot
// publication is atomic and readers never observe a busy inline-update state.
//
// DWHTMap is zero-value ready, but is intentionally excluded from race builds
// and currently requires amd64 or arm64 DWCAS support.
//
// WARNING on ARM64: The asm implementation relies on the ARMv8.1-A LSE CASPAL instruction.
// Running on older ARMv8.0 hardware without LSE will trigger a SIGILL (Illegal Instruction) crash.
type DWHTMap[K comparable, V any] struct {
	_         noCopy
	table     atomic.Pointer[dwhtTable[K, V]]
	initState atomic.Uint32
	intKey    bool
	seed      uintptr
	keyHash   HashFunc
	valEqual  EqualFunc
	minLen    uintptr
	size      PLocalCounterN
}

type dwhtTable[K comparable, V any] struct {
	slotsBase    unsafe.Pointer
	mask         uintptr
	probeLimit   uintptr
	stripeCap    int
	growCap      int
	chunkSz      uintptr
	chunks       uint32
	allocating   atomic.Uint32 // 0: no one is allocating, 1: allocating
	copyIdx      atomic.Uint32 // Next chunk index for cooperative resize
	copyDone     atomic.Uint32 // Number of resize chunks completed
	nextTable    atomic.Pointer[dwhtTable[K, V]]
	copyMaxProbe atomic.Uintptr // Max probe distance observed during resize copy
	slotsRaw     unsafe.Pointer // keeps the typed backing array alive for GC
}

type dwhtEntry[K comparable, V any] struct {
	key K
	val V
}

type dwhtSlotRaw struct {
	ctrl  uint64
	entry unsafe.Pointer
}

// DWCAS requires the two-word slot view to be 16-byte aligned. Go only
// guarantees pointer alignment for slice elements, so allocation tries both
// field orders and caches the one that is aligned for each power-of-two size.
type dwhtSlotRawRot8 struct {
	entry unsafe.Pointer
	ctrl  uint64
}

type dwhtSlotView struct {
	ctrl  uint64
	entry unsafe.Pointer
}

const dwhtSlotBytes = unsafe.Sizeof(dwhtSlotView{})

var (
	_ [16 - dwhtSlotBytes]byte
	_ [dwhtSlotBytes - 16]byte
)

const (
	dwhtSlotLayoutUnknown uint32 = iota
	dwhtSlotLayoutRaw
	dwhtSlotLayoutRot8
)

var dwhtSlotLayoutHints [bitSize]atomic.Uint32

//go:nosplit
func (t *dwhtTable[K, V]) slot(i uintptr) *dwhtSlotView {
	return (*dwhtSlotView)(unsafe.Add(t.slotsBase, i*dwhtSlotBytes))
}

const (
	dwhtStateMask    = uint64(0x3)
	dwhtStateEmpty   = uint64(0)
	dwhtStateFull    = uint64(1)
	dwhtStateDeleted = uint64(2)

	dwhtH2Shift = 2
	dwhtH2Mask  = uint64(0xFFFFFFFF)

	// ctrl layout:
	//   bits 0..1:   slot state
	//   bits 2..33:  32-bit hash fragment
	//   bits 34..62: sequence, bumped on every slot transition
	//   bit 63:      frozen during resize
	// The sequence is part of the DWCAS expected ctrl word, so stale CAS
	// attempts cannot succeed after a delete/revive/update cycle.
	dwhtSeqShift = 34
	dwhtSeqInc   = uint64(1) << dwhtSeqShift

	dwhtFrozen = uint64(1) << 63
)

type dwhtStoreStatus uint8

const (
	dwhtStoreOK dwhtStoreStatus = iota
	dwhtStoreFrozen
	dwhtStoreFull
)

type dwhtResizeHint uint8

const (
	dwhtResizeNormal dwhtResizeHint = iota
	dwhtResizeProbeLimit
)

const (
	// occupied tracks physical slots that are no longer Empty.
	dwhtCntOccupied int = iota
	dwhtCntTombstones
)

// NewDWHTMap creates an experimental DWHT-style map.
func NewDWHTMap[K comparable, V any](options ...func(*MapConfig)) *DWHTMap[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m := &DWHTMap[K, V]{}
	m.init(noEscape(&cfg))
	return m
}

func (m *DWHTMap[K, V]) init(cfg *MapConfig) {
	if cfg.keyHash == nil {
		cfg.keyHash = parseKeyInterface[K]()
	}
	if cfg.valEqual == nil {
		cfg.valEqual = parseValueInterface[V]()
	}

	m.keyHash, m.valEqual, m.intKey = defaultHasher[K, V]()
	if cfg.keyHash != nil {
		m.keyHash = cfg.keyHash
		m.intKey = false
	}
	if cfg.valEqual != nil {
		m.valEqual = cfg.valEqual
	}

	m.seed = uintptr(rand.Uint64())
	m.minLen = dwhtCalcSlotLen(cfg.capacity)
	m.table.Store(newDWHTTable[K, V](m.minLen))
}

// Load retrieves the value for a key.
func (m *DWHTMap[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	// Inline hashKey()
	var h uint32
	if dwhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = dwhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	start := dwhtStart(h, table.mask)
	limit := table.probeLimit
	for probe := uintptr(0); probe < limit; probe++ {
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUint64(&slot.ctrl)
		state := dwhtCtrlState(ctrl)
		if state == dwhtStateFull {
			if dwhtCtrlH2(ctrl) != h {
				continue
			}
			entryPtr := atomic.LoadPointer(&slot.entry)
			if entryPtr == nil {
				continue
			}
			entry := (*dwhtEntry[K, V])(entryPtr)
			if entry.key == key {
				return entry.val, true
			}
		} else if state == dwhtStateEmpty {
			return *new(V), false
		}
	}
	return *new(V), false
}

// Store sets the value for a key.
func (m *DWHTMap[K, V]) Store(key K, value V) {
	table := m.ensureTable()
	// Inline hashKey()
	var h uint32
	if dwhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = dwhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, _, _ := m.storeInto(table, noEscape(&key), noEscape(&value), h, false)
		switch status {
		case dwhtStoreOK:
			m.resizeIfNeeded(table)
			return
		case dwhtStoreFrozen:
			table = m.helpResize(table)
		case dwhtStoreFull:
			occupied := int(m.size.Value(dwhtCntOccupied))
			table = m.tryResize(table, occupied, dwhtResizeProbeLimit)
		}
	}
}

// LoadOrStore returns the existing value for the key if present. Otherwise it
// stores and returns the given value.
func (m *DWHTMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := m.ensureTable()
	// Inline hashKey()
	var h uint32
	if dwhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = dwhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, actual, loaded := m.storeInto(table, noEscape(&key), noEscape(&value), h, true)
		switch status {
		case dwhtStoreOK:
			if !loaded {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case dwhtStoreFrozen:
			table = m.helpResize(table)
		case dwhtStoreFull:
			occupied := int(m.size.Value(dwhtCntOccupied))
			table = m.tryResize(table, occupied, dwhtResizeProbeLimit)
		}
	}
}

// LoadAndUpdate retrieves the value for a key and updates it if the key exists.
func (m *DWHTMap[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	// Inline hashKey()
	var h uint32
	if dwhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = dwhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, prev, loaded := m.loadAndUpdateIn(table, noEscape(&key), noEscape(&value), h)
		switch status {
		case dwhtStoreOK:
			return prev, loaded
		case dwhtStoreFrozen:
			table = m.helpResize(table)
		case dwhtStoreFull:
			occupied := int(m.size.Value(dwhtCntOccupied))
			table = m.tryResize(table, occupied, dwhtResizeProbeLimit)
		}
	}
}

// Delete removes the value for a key.
func (m *DWHTMap[K, V]) Delete(key K) {
	table := m.table.Load()
	if table == nil {
		return
	}
	// Inline hashKey()
	var h uint32
	if dwhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = dwhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, _, _ := m.deleteFrom(table, noEscape(&key), h, false)
		if status == dwhtStoreOK {
			return
		}
		table = m.helpResize(table)
	}
}

// LoadAndDelete retrieves the value for a key and deletes it from the map.
func (m *DWHTMap[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	// Inline hashKey()
	var h uint32
	if dwhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = dwhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, prev, loaded := m.deleteFrom(table, noEscape(&key), h, true)
		if status == dwhtStoreOK {
			return prev, loaded
		}
		table = m.helpResize(table)
	}
}

// CompareAndSwap atomically replaces an existing value with a new value.
func (m *DWHTMap[K, V]) CompareAndSwap(key K, old V, new V) bool {
	table := m.table.Load()
	if table == nil {
		return false
	}
	// Inline hashKey()
	var h uint32
	if dwhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = dwhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, swapped := m.compareAndSwapIn(
			table,
			noEscape(&key),
			noEscape(&old),
			noEscape(&new),
			h,
		)
		if status == dwhtStoreOK {
			return swapped
		}
		table = m.helpResize(table)
	}
}

// CompareAndDelete atomically deletes an existing entry.
func (m *DWHTMap[K, V]) CompareAndDelete(key K, old V) bool {
	table := m.table.Load()
	if table == nil {
		return false
	}
	// Inline hashKey()
	var h uint32
	if dwhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = dwhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, deleted := m.compareAndDeleteIn(
			table,
			noEscape(&key),
			noEscape(&old),
			h,
		)
		if status == dwhtStoreOK {
			return deleted
		}
		table = m.helpResize(table)
	}
}

// Range iterates weakly over the table pointer observed at entry.
// It does not help an in-flight resize and may miss entries inserted into a
// next table after this call starts.
func (m *DWHTMap[K, V]) Range(yield func(K, V) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}
	for i := uintptr(0); i <= table.mask; i++ {
		slot := table.slot(i)
		ctrl := atomic.LoadUint64(&slot.ctrl)
		if dwhtCtrlState(ctrl) != dwhtStateFull {
			continue
		}
		entryPtr := atomic.LoadPointer(&slot.entry)
		if entryPtr == nil {
			continue
		}
		entry := (*dwhtEntry[K, V])(entryPtr)
		if !yield(entry.key, entry.val) {
			return
		}
	}
}

// All returns an iterator function for use with range-over-func.
//
//go:nosplit
func (m *DWHTMap[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the approximate number of entries in the map.
func (m *DWHTMap[K, V]) Size() int {
	table := m.table.Load()
	if table == nil {
		return 0
	}
	occupied := int(m.size.Value(dwhtCntOccupied))
	tombstones := int(m.size.Value(dwhtCntTombstones))
	return max(occupied-tombstones, 0)
}

// Clear clears all key-value pairs from the map.
func (m *DWHTMap[K, V]) Clear() {
	table := m.table.Load()
	if table == nil {
		return
	}
	m.table.Store(newDWHTTable[K, V](m.minLen))
	m.size.Clear()
}

func (m *DWHTMap[K, V]) ensureTable() *dwhtTable[K, V] {
	table := m.table.Load()
	if table != nil {
		return table
	}
	return m.slowInit()
}

//go:noinline
func (m *DWHTMap[K, V]) slowInit() *dwhtTable[K, V] {
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

func (m *DWHTMap[K, V]) storeInto(
	table *dwhtTable[K, V],
	key *K,
	val *V,
	h uint32,
	onlyIfAbsent bool,
) (dwhtStoreStatus, V, bool) {
	var newEntry *dwhtEntry[K, V]
	start := dwhtStart(h, table.mask)
	limit := table.probeLimit
	for probe := uintptr(0); probe < limit; probe++ {
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUint64(&slot.ctrl)
		if ctrl&dwhtFrozen != 0 {
			return dwhtStoreFrozen, *new(V), false
		}
		state := dwhtCtrlState(ctrl)
		if state == dwhtStateFull {
			if dwhtCtrlH2(ctrl) != h {
				continue
			}
			entry := atomic.LoadPointer(&slot.entry)
			if entry == nil {
				continue
			}
			e := (*dwhtEntry[K, V])(entry)
			if e.key != *key {
				continue
			}
			if onlyIfAbsent {
				return dwhtStoreOK, e.val, true
			}
			if dwhtEnableDedupVal {
				if m.valEqual != nil && m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return dwhtStoreOK, e.val, true
				}
			}

			if newEntry == nil {
				newEntry = &dwhtEntry[K, V]{key: *key, val: *val}
			}
			newCtrl := dwhtCtrlUpdate(ctrl)
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, entry, newCtrl, unsafe.Pointer(newEntry)) {
				probe--
				continue
			}
			return dwhtStoreOK, *val, true
		} else if state == dwhtStateDeleted {
			// Only the same key may revive its own tombstone. Reusing an
			// arbitrary Deleted slot is unsafe: another goroutine may have
			// inserted the same key earlier in the probe sequence meanwhile.
			if dwhtCtrlH2(ctrl) != h {
				continue
			}
			entry := atomic.LoadPointer(&slot.entry)
			if entry == nil {
				continue
			}
			e := (*dwhtEntry[K, V])(entry)
			if e.key != *key {
				continue
			}
			if newEntry == nil {
				newEntry = &dwhtEntry[K, V]{key: *key, val: *val}
			}
			newCtrl := dwhtCtrlInsert(ctrl, h)
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, entry, newCtrl, unsafe.Pointer(newEntry)) {
				probe--
				continue
			}
			m.size.Add(dwhtCntTombstones, ^uintptr(0))
			return dwhtStoreOK, *val, false
		} else if state == dwhtStateEmpty {
			entry := atomic.LoadPointer(&slot.entry)
			if newEntry == nil {
				newEntry = &dwhtEntry[K, V]{key: *key, val: *val}
			}
			newCtrl := dwhtCtrlInsert(ctrl, h)
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, entry, newCtrl, unsafe.Pointer(newEntry)) {
				probe--
				continue
			}
			m.size.Add(dwhtCntOccupied, 1)
			return dwhtStoreOK, *val, false
		}
	}
	return dwhtStoreFull, *new(V), false
}

func (m *DWHTMap[K, V]) loadAndUpdateIn(
	table *dwhtTable[K, V],
	key *K,
	val *V,
	h uint32,
) (dwhtStoreStatus, V, bool) {
	var newEntry *dwhtEntry[K, V]
	start := dwhtStart(h, table.mask)
	limit := table.probeLimit
	for probe := uintptr(0); probe < limit; probe++ {
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUint64(&slot.ctrl)
		if ctrl&dwhtFrozen != 0 {
			return dwhtStoreFrozen, *new(V), false
		}
		state := dwhtCtrlState(ctrl)
		if state == dwhtStateFull {
			if dwhtCtrlH2(ctrl) != h {
				continue
			}
			entry := atomic.LoadPointer(&slot.entry)
			if entry == nil {
				continue
			}
			e := (*dwhtEntry[K, V])(entry)
			if e.key != *key {
				continue
			}
			if dwhtEnableDedupVal {
				if m.valEqual != nil && m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return dwhtStoreOK, e.val, true
				}
			}
			if newEntry == nil {
				newEntry = &dwhtEntry[K, V]{key: *key, val: *val}
			}
			newCtrl := dwhtCtrlUpdate(ctrl)
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, entry, newCtrl, unsafe.Pointer(newEntry)) {
				probe--
				continue
			}
			return dwhtStoreOK, e.val, true
		} else if state == dwhtStateEmpty {
			return dwhtStoreOK, *new(V), false
		}
	}
	return dwhtStoreOK, *new(V), false
}

func (m *DWHTMap[K, V]) deleteFrom(
	table *dwhtTable[K, V],
	key *K,
	h uint32,
	needValue bool,
) (dwhtStoreStatus, V, bool) {
	start := dwhtStart(h, table.mask)
	limit := table.probeLimit
	for probe := uintptr(0); probe < limit; probe++ {
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUint64(&slot.ctrl)
		if ctrl&dwhtFrozen != 0 {
			return dwhtStoreFrozen, *new(V), false
		}
		state := dwhtCtrlState(ctrl)
		if state == dwhtStateFull {
			if dwhtCtrlH2(ctrl) != h {
				continue
			}
			entry := atomic.LoadPointer(&slot.entry)
			if entry == nil {
				continue
			}
			e := (*dwhtEntry[K, V])(entry)
			if e.key != *key {
				continue
			}

			newCtrl := dwhtCtrlDelete(ctrl)
			// Keep entry attached to the tombstone. The key is needed to allow
			// same-key revival without permitting arbitrary tombstone reuse.
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, entry, newCtrl, entry) {
				probe--
				continue
			}

			var prev V
			if needValue {
				prev = e.val
			}
			m.size.Add(dwhtCntTombstones, 1)
			return dwhtStoreOK, prev, true
		} else if state == dwhtStateEmpty {
			return dwhtStoreOK, *new(V), false
		}
	}
	return dwhtStoreOK, *new(V), false
}

func (m *DWHTMap[K, V]) compareAndSwapIn(
	table *dwhtTable[K, V],
	key *K,
	old *V,
	newVal *V,
	h uint32,
) (dwhtStoreStatus, bool) {
	var newEntry *dwhtEntry[K, V]
	start := dwhtStart(h, table.mask)
	limit := table.probeLimit
	for probe := uintptr(0); probe < limit; probe++ {
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUint64(&slot.ctrl)
		if ctrl&dwhtFrozen != 0 {
			return dwhtStoreFrozen, false
		}
		state := dwhtCtrlState(ctrl)
		if state == dwhtStateFull {
			if dwhtCtrlH2(ctrl) != h {
				continue
			}
			entry := atomic.LoadPointer(&slot.entry)
			if entry == nil {
				continue
			}
			e := (*dwhtEntry[K, V])(entry)
			if e.key != *key {
				continue
			}
			if m.valEqual == nil {
				panicDWHTValueNotComparable()
			}
			if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
				return dwhtStoreOK, false
			}

			if newEntry == nil {
				newEntry = &dwhtEntry[K, V]{key: *key, val: *newVal}
			}
			newCtrl := dwhtCtrlUpdate(ctrl)
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, entry, newCtrl, unsafe.Pointer(newEntry)) {
				probe--
				continue
			}
			return dwhtStoreOK, true
		} else if state == dwhtStateEmpty {
			return dwhtStoreOK, false
		}
	}
	return dwhtStoreOK, false
}

func (m *DWHTMap[K, V]) compareAndDeleteIn(
	table *dwhtTable[K, V],
	key *K,
	old *V,
	h uint32,
) (dwhtStoreStatus, bool) {
	start := dwhtStart(h, table.mask)
	limit := table.probeLimit
	for probe := uintptr(0); probe < limit; probe++ {
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUint64(&slot.ctrl)
		if ctrl&dwhtFrozen != 0 {
			return dwhtStoreFrozen, false
		}
		state := dwhtCtrlState(ctrl)
		if state == dwhtStateFull {
			if dwhtCtrlH2(ctrl) != h {
				continue
			}
			entry := atomic.LoadPointer(&slot.entry)
			if entry == nil {
				continue
			}
			e := (*dwhtEntry[K, V])(entry)
			if e.key != *key {
				continue
			}
			if m.valEqual == nil {
				panicDWHTValueNotComparable()
			}
			if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
				return dwhtStoreOK, false
			}

			newCtrl := dwhtCtrlDelete(ctrl)
			// Keep entry attached to the tombstone for the same reason as
			// deleteFrom: only the original key may revive this slot.
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, entry, newCtrl, entry) {
				probe--
				continue
			}

			m.size.Add(dwhtCntTombstones, 1)
			return dwhtStoreOK, true
		} else if state == dwhtStateEmpty {
			return dwhtStoreOK, false
		}
	}
	return dwhtStoreOK, false
}

func (m *DWHTMap[K, V]) resizeIfNeeded(table *dwhtTable[K, V]) {
	localSize := int(m.size.Get(dwhtCntOccupied))
	if localSize&dwhtGrowCheckMask != 0 {
		return
	}
	if localSize < table.stripeCap {
		return
	}
	if table.allocating.Load() != 0 {
		return
	}
	occupied := int(m.size.Value(dwhtCntOccupied))
	if occupied >= table.growCap {
		m.tryResize(table, occupied, dwhtResizeNormal)
	}
}

func (m *DWHTMap[K, V]) tryResize(old *dwhtTable[K, V], occupied int, hint dwhtResizeHint) *dwhtTable[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}
	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			tombstones := int(m.size.Value(dwhtCntTombstones))
			live := occupied - tombstones
			// Base sizing follows the live entry count. At the normal grow
			// threshold this rounds to 2x; tombstone-heavy resize can stay at
			// the same size and compact deleted slots away.
			nextLen := dwhtCalcSlotLen(live)
			nextLen = max(nextLen, m.minLen)
			aggressive := hint == dwhtResizeProbeLimit
			if dwhtEnableAggressiveGrow {
				curOccupied := int(m.size.Value(dwhtCntOccupied))
				occupiedInResize := curOccupied - occupied
				aggressive = aggressive || occupiedInResize >= 2
			}
			// Probe-limit resize or observed concurrent insert pressure gets
			// one extra size class, capped at 4x the old table.
			if aggressive {
				slotLen := old.mask + 1
				nextLen = min(nextLen<<1, slotLen<<2)
			}

			newTable := newDWHTTable[K, V](nextLen)
			old.nextTable.CompareAndSwap(nil, newTable)
			next = old.nextTable.Load()
		} else {
			if dwhtEnableStoreInGrow {
				// Let the caller retry the write against old while the resize
				// leader allocates nextTable. Frozen slots will force helping.
				return old
			}
			// Wait for leader to allocate
			for {
				next = old.nextTable.Load()
				if next != nil {
					break
				}
				runtime.Gosched()
			}
		}
	}
	return m.helpResizeInto(old, next)
}

func (m *DWHTMap[K, V]) helpResize(old *dwhtTable[K, V]) *dwhtTable[K, V] {
	for {
		if table := m.table.Load(); table != old {
			return table
		}

		next := old.nextTable.Load()
		if next == nil {
			runtime.Gosched()
			continue
		}
		return m.helpResizeInto(old, next)
	}
}

func (m *DWHTMap[K, V]) helpResizeInto(old, next *dwhtTable[K, V]) *dwhtTable[K, V] {
	slotLen := old.mask + 1 // power of two
	chunks, chunkSz := old.chunks, old.chunkSz
	nextLen := next.mask + 1
	// Cooperative resize.
	// Each slot is frozen before it is copied. This removes the old global
	// freeze barrier while preserving the key rule: a copied slot can no longer
	// be modified in the old table.
	for {
		chunk := old.copyIdx.Add(1) - 1
		if chunk >= chunks {
			// Wait for leader to finish.
			for {
				table := m.table.Load()
				if table != old {
					return table
				}
				runtime.Gosched()
			}
		}
		var copyMaxProbe uintptr
		start := uintptr(chunk) * chunkSz
		end := min(start+chunkSz, slotLen)
		for i := start; i < end; i++ {
			slot := old.slot(i)
			var ctrl uint64
			var entry unsafe.Pointer
			for {
				ctrl = atomic.LoadUint64(&slot.ctrl)
				if ctrl&dwhtFrozen != 0 {
					entry = atomic.LoadPointer(&slot.entry)
					break
				}
				entry = atomic.LoadPointer(&slot.entry)
				if !asm.DWCAS(unsafe.Pointer(slot), ctrl, entry, ctrl|dwhtFrozen, entry) {
					continue
				}
				ctrl |= dwhtFrozen
				break
			}

			if dwhtCtrlState(ctrl) != dwhtStateFull {
				continue
			}
			entryPtr := entry
			if entryPtr == nil {
				continue
			}

			h := dwhtCtrlH2(ctrl)
			destStart := dwhtStart(h, next.mask)
			probe := uintptr(0)
			for ; probe < nextLen; probe++ {
				destSlot := next.slot((destStart + probe) & next.mask)
				destCtrl := atomic.LoadUint64(&destSlot.ctrl)
				if dwhtCtrlState(destCtrl) != dwhtStateEmpty {
					continue
				}
				destEntry := atomic.LoadPointer(&destSlot.entry)
				newCtrl := dwhtCtrlInsert(destCtrl, h)
				if !asm.DWCAS(unsafe.Pointer(destSlot), destCtrl, destEntry, newCtrl, entryPtr) {
					probe--
					continue
				}
				break
			}
			if probe >= nextLen {
				panicDWHTGrowExceededMaxProbeThreshold()
			}
			if probe > copyMaxProbe {
				copyMaxProbe = probe
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
		if old.copyDone.Add(1) == chunks {
			occupied := m.size.Reset(dwhtCntOccupied)
			tombstones := m.size.Reset(dwhtCntTombstones)
			m.size.Add(dwhtCntOccupied, occupied-tombstones)
			// Adaptive probe limit: tighten based on the max probe distance
			// actually observed during migration. This allows the window to
			// shrink when clustering dissipates after table growth or
			// tombstone compaction, avoiding a permanently inflated miss path.
			observed := next.copyMaxProbe.Load() + 1
			next.probeLimit = min(nextLen, nextPowOf2(max(observed<<1, dwhtMaxProbeThreshold)))
			m.table.CompareAndSwap(old, next)
			return next
		}
	}
}

func newDWHTTable[K comparable, V any](slotLen uintptr) *dwhtTable[K, V] {
	slotLen = nextPowOf2(max(slotLen, dwhtMinSlots))
	probeLimit := min(slotLen, uintptr(dwhtMaxProbeThreshold))
	base, raw := makeDWHTSlots(slotLen)
	growCap := int(float64(slotLen) * dwhtLoadFactor)
	cpus := maxProcs()
	roundedSizeLen := nextPowOf2(cpus)
	stripeCap := int(growCap >> bits.TrailingZeros32(uint32(roundedSizeLen)))
	chunks, chunkSz := dwhtResizeChunks(slotLen, cpus)
	return &dwhtTable[K, V]{
		slotsBase:  base,
		slotsRaw:   raw,
		mask:       slotLen - 1,
		probeLimit: probeLimit,
		stripeCap:  stripeCap,
		growCap:    growCap,
		chunks:     chunks,
		chunkSz:    chunkSz,
	}
}

func makeDWHTSlots(slotLen uintptr) (unsafe.Pointer, unsafe.Pointer) {
	hint := &dwhtSlotLayoutHints[bits.TrailingZeros(uint(slotLen))]
	triedRaw := false
	triedRot8 := false
	switch hint.Load() {
	case dwhtSlotLayoutRaw:
		triedRaw = true
		if base, raw, ok := makeDWHTRawSlots(slotLen); ok {
			return base, raw
		}
	case dwhtSlotLayoutRot8:
		triedRot8 = true
		if base, raw, ok := makeDWHTRot8Slots(slotLen); ok {
			return base, raw
		}
	}

	if !triedRaw {
		if base, raw, ok := makeDWHTRawSlots(slotLen); ok {
			hint.Store(dwhtSlotLayoutRaw)
			return base, raw
		}
	}
	if !triedRot8 {
		if base, raw, ok := makeDWHTRot8Slots(slotLen); ok {
			hint.Store(dwhtSlotLayoutRot8)
			return base, raw
		}
	}

	panicDWHTSlotStorageMisaligned()
	return nil, nil
}

func makeDWHTRawSlots(slotLen uintptr) (unsafe.Pointer, unsafe.Pointer, bool) {
	raw := make([]dwhtSlotRaw, int(slotLen))
	base := unsafe.Pointer(&raw[0].ctrl)
	if uintptr(base)&(dwhtSlotBytes-1) == 0 {
		return base, unsafe.Pointer(unsafe.SliceData(raw)), true
	}
	return nil, nil, false
}

func makeDWHTRot8Slots(slotLen uintptr) (unsafe.Pointer, unsafe.Pointer, bool) {
	rot := make([]dwhtSlotRawRot8, int(slotLen)+1)
	base := unsafe.Pointer(&rot[0].ctrl)
	if uintptr(base)&(dwhtSlotBytes-1) == 0 {
		return base, unsafe.Pointer(unsafe.SliceData(rot)), true
	}
	return nil, nil, false
}

//go:nosplit
func dwhtCalcSlotLen(capacity int) uintptr {
	if capacity <= 0 {
		return dwhtMinSlots
	}
	const invLoadFactor = 1 / dwhtLoadFactor
	need := uintptr(float64(capacity+1) * invLoadFactor)
	return nextPowOf2(max(need, dwhtMinSlots))
}

//go:nosplit
func dwhtResizeChunks(slotLen, cpus uintptr) (chunks uint32, chunkSz uintptr) {
	const overCpus = resizeOverPartition
	const minSlotsPerCpu = 1
	want := min(cpus*overCpus, slotLen/minSlotsPerCpu) // slotLen is power-of-two
	if want <= 1 {
		return 1, slotLen
	}
	c := uint32(1) << (bits.Len32(uint32(want)) - 1) // floorPow2(want)
	return c, slotLen >> bits.TrailingZeros32(c)
}

//go:nosplit
func dwhtStart(h1v uint32, mask uintptr) uintptr {
	return uintptr(h1v) & mask
}

//go:nosplit
func dwhtCtrlState(ctrl uint64) uint64 {
	return ctrl & dwhtStateMask
}

//go:nosplit
func dwhtCtrlH2(ctrl uint64) uint32 {
	return uint32(ctrl >> dwhtH2Shift)
}

//go:nosplit
func dwhtCtrlInsert(ctrl uint64, h2v uint32) uint64 {
	c := (ctrl &^ dwhtStateMask) | dwhtStateFull
	c = (c &^ (dwhtH2Mask << dwhtH2Shift)) | (uint64(h2v) << dwhtH2Shift)
	return c + dwhtSeqInc
}

//go:nosplit
func dwhtCtrlUpdate(ctrl uint64) uint64 {
	return (ctrl+dwhtSeqInc)&^dwhtStateMask | dwhtStateFull
}

//go:nosplit
func dwhtCtrlDelete(ctrl uint64) uint64 {
	return (ctrl+dwhtSeqInc)&^dwhtStateMask | dwhtStateDeleted
}

//go:nosplit
func dwhtIntHash(x uintptr) uint32 {
	if bitSize == 32 {
		return uint32(x) * uint32(0x9e3779b9)
	}
	return uint32((uint64(x) * uint64(0x9e3779b97f4a7c15)) >> 32)
}

//go:noinline
func panicDWHTValueNotComparable() {
	panic("cc: value is not comparable; use WithValueEqual")
}

//go:noinline
func panicDWHTGrowExceededMaxProbeThreshold() {
	panic("cc: DWHTMap grow exceeded max probe threshold")
}

//go:noinline
func panicDWHTSlotStorageMisaligned() {
	panic("cc: DWHTMap slot storage is not 16-byte aligned")
}
