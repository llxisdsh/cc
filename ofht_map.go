package cc

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	ofhtEnableIntKey   = false
	ofhtEnableDedupVal = false
)

// OFHTMap is an experimental DHLT-style hash map (Optimistic Flat Hash Table).
//
// The implementation is intentionally isolated from Map and FlatMap. It uses
// open addressing, cache-line friendly slot groups, and per-slot CAS state
// transitions instead of root-bucket locks. The current Go implementation uses
// a single-word slot control CAS plus a short "busy" state to coordinate
// concurrent updates to the inline key and value slots. This contrasts with
// pointer-based designs where a hardware DWCAS (Double-Word CAS) could
// atomically publish both a control word and an *entry pointer, completely
// eliminating the "busy" state. Since this map inlines generic K/V of
// arbitrary sizes, the two-phase SeqLock pattern is strictly required.
//
// Concurrency model:
//   - Loads are lock-free and only spin when they observe a slot being updated
//     (the "busy" state).
//   - Writes reserve one slot with CAS, write the key and value directly into
//     the slot, then release the slot by storing a full control word.
//   - Grow freezes the old table slot-by-slot before copying. Writers that hit
//     a frozen slot retry on the newly published table.
//
// This is a prototype for exploring DHLT-style write scalability. It implements
// most of the standard Map API surface but lacks Compute-style operations and
// lock-free cooperative resizing.
type OFHTMap[K comparable, V any] struct {
	_        noCopy
	table    atomic.Pointer[ofhtTable[K, V]]
	intKey   bool
	seed     uintptr
	keyHash  HashFunc
	valEqual EqualFunc
	minLen   uintptr
	size     PLocalCounter
}

type ofhtTable[K comparable, V any] struct {
	slots      unsafeSlice[ofhtSlot[K, V]]
	mask       uintptr
	stripeCap  int
	growCap    uintptr
	nextTable  atomic.Pointer[ofhtTable[K, V]]
	allocating atomic.Uint32 // 0: no one is allocating, 1: allocating
	freezeIdx  atomic.Uint32 // Next chunk index for cooperative resizing (Phase 1: freeze)
	frozenDone atomic.Uint32 // Number of freeze chunks completed
	copyIdx    atomic.Uint32 // Next chunk index for cooperative resizing (Phase 2: copy)
	copyDone   atomic.Uint32 // Number of copy chunks completed
}

type ofhtSlot[K comparable, V any] struct {
	ctrl atomic.Uint64
	key  SeqLockSlot[K]
	val  SeqLockSlot[V]
}

const (
	ofhtStateMask    = uint64(0x3)
	ofhtStateEmpty   = uint64(0)
	ofhtStateFull    = uint64(1)
	ofhtStateDeleted = uint64(2)
	ofhtStateBusy    = uint64(3)

	ofhtH2Shift = 2
	ofhtH2Mask  = uint64(0xFFFF)

	ofhtSeqShift = 18
	ofhtSeqInc   = uint64(1) << ofhtSeqShift

	ofhtFrozen = uint64(1) << 63
)

const (
	ofhtMinSlots   = 64
	ofhtLoadFactor = 0.75

	// ofhtMaxProbeThreshold is the threshold of linear probing depth.
	// If a store operation probes more than this many slots without success,
	// it will eagerly trigger a resize even if the table is not fully loaded.
	ofhtMaxProbeThreshold = 128

	// ofhtGrowCheckMask is used as a bitwise AND mask to sample the local size counter.
	// This reduces the overhead of checking the global size on every insertion.
	// It MUST be strictly smaller than the initial grow threshold.
	// Since ofhtMinSlots is 64 and the grow factor is 3/4, the first grow
	// happens at 48. If this mask is too large (e.g., 63), the table could fill up
	// completely without triggering a resize in highly concurrent cold starts.
	ofhtGrowCheckMask = 7 // Checks every 8th local insert
)

type ofhtStoreStatus uint8

const (
	ofhtStoreOK ofhtStoreStatus = iota
	ofhtStoreFrozen
	ofhtStoreFull
	ofhtStoreRetry
)

// NewOFHTMap creates an experimental OFHT-style map.
func NewOFHTMap[K comparable, V any](options ...func(*MapConfig)) *OFHTMap[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m := &OFHTMap[K, V]{}
	m.init(noEscape(&cfg))
	return m
}

func (m *OFHTMap[K, V]) init(cfg *MapConfig) {
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
	m.minLen = ofhtCalcSlotLen(cfg.capacity)
	m.table.Store(newOFHTTable[K, V](m.minLen))
}

// Load retrieves the value for a key.
func (m *OFHTMap[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	return m.loadFrom(table, noEscape(&key), h1v, h2v)
}

// Store sets the value for a key.
func (m *OFHTMap[K, V]) Store(key K, value V) {
	table := m.ensureTable()
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, _, _ := m.storeInto(table, noEscape(&key), noEscape(&value), h1v, h2v, false)
		switch status {
		case ofhtStoreOK:
			m.growIfNeeded(table)
			return
		case ofhtStoreFrozen:
			table = m.afterFrozenTable(table)
		case ofhtStoreFull:
			m.tryGrow(table, (table.mask+1)<<1)
			table = m.ensureTable()
		case ofhtStoreRetry:
			table = m.ensureTable()
		}
	}
}

// LoadOrStore returns the existing value for the key if present. Otherwise it
// stores and returns the given value.
func (m *OFHTMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := m.ensureTable()
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, actual, loaded := m.storeInto(table, noEscape(&key), noEscape(&value), h1v, h2v, true)
		switch status {
		case ofhtStoreOK:
			if !loaded {
				m.growIfNeeded(table)
			}
			return actual, loaded
		case ofhtStoreFrozen:
			table = m.afterFrozenTable(table)
		case ofhtStoreFull:
			m.tryGrow(table, (table.mask+1)<<1)
			table = m.ensureTable()
		case ofhtStoreRetry:
			table = m.ensureTable()
		}
	}
}

// LoadAndUpdate retrieves the value for a key and updates it if the key exists.
func (m *OFHTMap[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, prev, loaded := m.loadAndUpdateIn(table, noEscape(&key), noEscape(&value), h1v, h2v)
		switch status {
		case ofhtStoreOK:
			return prev, loaded
		case ofhtStoreFrozen:
			table = m.afterFrozenTable(table)
		case ofhtStoreFull:
			m.tryGrow(table, (table.mask+1)<<1)
			table = m.ensureTable()
		case ofhtStoreRetry:
			table = m.ensureTable()
		}
	}
}

// Delete removes the value for a key.
func (m *OFHTMap[K, V]) Delete(key K) {
	table := m.table.Load()
	if table == nil {
		return
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, _, _ := m.deleteFrom(table, noEscape(&key), h1v, h2v, false)
		if status == ofhtStoreOK {
			return
		}
		table = m.afterFrozenTable(table)
	}
}

// LoadAndDelete retrieves the value for a key and deletes it from the map.
func (m *OFHTMap[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, prev, loaded := m.deleteFrom(table, noEscape(&key), h1v, h2v, true)
		if status == ofhtStoreOK {
			return prev, loaded
		}
		table = m.afterFrozenTable(table)
	}
}

// CompareAndSwap atomically replaces an existing value with a new value.
func (m *OFHTMap[K, V]) CompareAndSwap(key K, old V, new V) bool {
	table := m.table.Load()
	if table == nil {
		return false
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, swapped := m.compareAndSwapIn(
			table,
			noEscape(&key),
			noEscape(&old),
			noEscape(&new),
			h1v,
			h2v,
		)
		if status == ofhtStoreOK {
			return swapped
		}
		table = m.afterFrozenTable(table)
	}
}

// CompareAndDelete atomically deletes an existing entry.
// If its value matches the expected value.
func (m *OFHTMap[K, V]) CompareAndDelete(key K, old V) bool {
	table := m.table.Load()
	if table == nil {
		return false
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, deleted := m.compareAndDeleteIn(
			table,
			noEscape(&key),
			noEscape(&old),
			h1v,
			h2v,
		)
		if status == ofhtStoreOK {
			return deleted
		}
		table = m.afterFrozenTable(table)
	}
}

// Range iterates over a weakly consistent snapshot of the table.
func (m *OFHTMap[K, V]) Range(yield func(K, V) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}
	for i := uintptr(0); i <= table.mask; i++ {
		slot := table.slots.At(i)
	retry:
		for {
			ctrl := slot.ctrl.Load()
			if ofhtCtrlState(ctrl) != ofhtStateFull {
				break
			}
			k := slot.key.ReadUnfenced()
			v := slot.val.ReadUnfenced()
			ctrl2 := slot.ctrl.Load()
			if ctrl != ctrl2 {
				continue retry
			}
			if !yield(k, v) {
				return
			}
			break
		}
	}
}

// All returns an iterator function for use with range-over-func.
//
//go:nosplit
func (m *OFHTMap[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the approximate number of entries in the map.
func (m *OFHTMap[K, V]) Size() int {
	return int(m.size.Value())
}

func (m *OFHTMap[K, V]) ensureTable() *ofhtTable[K, V] {
	table := m.table.Load()
	if table != nil {
		return table
	}
	// Lock-free init
	newTab := newOFHTTable[K, V](m.minLen)
	if m.table.CompareAndSwap(nil, newTab) {
		return newTab
	}
	return m.table.Load()
}

//go:nosplit
func (m *OFHTMap[K, V]) hashKey(key *K) (uintptr, uint16) {
	if ofhtEnableIntKey {
		if m.intKey {
			h1v := intHash[K](noescape(unsafe.Pointer(key)))
			return h1v, uint16(h1v ^ (h1v >> 16))
		}
	}
	h1v := m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
	return h1v >> 16, uint16(h1v)
}

func (m *OFHTMap[K, V]) loadFrom(
	table *ofhtTable[K, V],
	key *K,
	h1v uintptr,
	h2v uint16,
) (value V, ok bool) {
	// var spins int
	start := ofhtStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		// Eagerly trigger a resize if the probe sequence is too long,
		// preventing severe performance degradation due to clustering.
		if probe > ofhtMaxProbeThreshold {
			return *new(V), false
		}
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		switch ofhtCtrlState(ctrl) {
		case ofhtStateEmpty:
			return *new(V), false
		case ofhtStateBusy:
			// delay(&spins)
			probe--
			continue
		case ofhtStateFull:
			if ofhtCtrlH2(ctrl) != h2v {
				continue
			}
			k := slot.key.ReadUnfenced()
			v := slot.val.ReadUnfenced()
			ctrl2 := slot.ctrl.Load()
			if ctrl == ctrl2 {
				if k == *key {
					return v, true
				}
			} else {
				// Slot was concurrently modified, retry this slot
				// delay(&spins)
				probe--
				continue
			}
		}
	}
	return *new(V), false
}

func (m *OFHTMap[K, V]) storeInto(
	table *ofhtTable[K, V],
	key *K,
	val *V,
	h1v uintptr,
	h2v uint16,
	onlyIfAbsent bool,
) (ofhtStoreStatus, V, bool) {
	var (
		// spins     int
		deleted   *ofhtSlot[K, V]
		deletedC  uint64
		deletedOK bool
	)
	start := ofhtStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		// Eagerly trigger a resize if the probe sequence is too long,
		// preventing severe performance degradation due to clustering.
		if probe > ofhtMaxProbeThreshold {
			m.tryGrow(table, (table.mask+1)<<1)
			return ofhtStoreRetry, *new(V), false
		}
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, *new(V), false
		}
		switch ofhtCtrlState(ctrl) {
		case ofhtStateBusy:
			// delay(&spins)
			probe--
			continue
		case ofhtStateFull:
			if ofhtCtrlH2(ctrl) != h2v {
				continue
			}
			k := slot.key.ReadUnfenced()
			v := slot.val.ReadUnfenced()
			ctrl2 := slot.ctrl.Load()
			if ctrl != ctrl2 {
				// delay(&spins)
				probe--
				continue
			}
			if k != *key {
				continue
			}
			if onlyIfAbsent {
				return ofhtStoreOK, v, true
			}
			if ofhtEnableDedupVal {
				if m.valEqual != nil && m.valEqual(noescape(unsafe.Pointer(&v)), noescape(unsafe.Pointer(val))) {
					return ofhtStoreOK, v, true
				}
			}
			busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}
			slot.val.WriteUnfenced(*val)
			slot.ctrl.Store(ofhtCtrlUpdate(busyCtrl))
			return ofhtStoreOK, *val, true
		case ofhtStateDeleted:
			if !deletedOK {
				deleted, deletedC, deletedOK = slot, ctrl, true
			}
		case ofhtStateEmpty:
			if deletedOK {
				status, rVal, loaded := m.claimSlot(deleted, deletedC, key, val, h2v)
				if status == ofhtStoreRetry {
					return ofhtStoreRetry, *new(V), false
				}
				return status, rVal, loaded
			}
			busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}
			slot.key.WriteUnfenced(*key)
			slot.val.WriteUnfenced(*val)
			slot.ctrl.Store(ofhtCtrlInsert(busyCtrl, h2v))
			m.size.Add(1)
			return ofhtStoreOK, *val, false
		}
	}
	if deletedOK {
		return m.claimSlot(deleted, deletedC, key, val, h2v)
	}
	return ofhtStoreFull, *new(V), false
}

func (m *OFHTMap[K, V]) claimSlot(
	slot *ofhtSlot[K, V],
	ctrl uint64,
	key *K,
	val *V,
	h2v uint16,
) (ofhtStoreStatus, V, bool) {
	if ctrl&ofhtFrozen != 0 {
		return ofhtStoreFrozen, *new(V), false
	}
	busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
	if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
		return ofhtStoreRetry, *new(V), false
	}
	slot.key.WriteUnfenced(*key)
	slot.val.WriteUnfenced(*val)
	slot.ctrl.Store(ofhtCtrlInsert(busyCtrl, h2v))
	m.size.Add(1)
	return ofhtStoreOK, *val, false
}

func (m *OFHTMap[K, V]) loadAndUpdateIn(
	table *ofhtTable[K, V],
	key *K,
	val *V,
	h1v uintptr,
	h2v uint16,
) (ofhtStoreStatus, V, bool) {
	start := ofhtStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > ofhtMaxProbeThreshold {
			return ofhtStoreFull, *new(V), false
		}
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, *new(V), false
		}
		switch ofhtCtrlState(ctrl) {
		case ofhtStateEmpty:
			return ofhtStoreOK, *new(V), false
		case ofhtStateBusy:
			probe--
			continue
		case ofhtStateFull:
			if ofhtCtrlH2(ctrl) != h2v {
				continue
			}
			k := slot.key.ReadUnfenced()
			prev := slot.val.ReadUnfenced()
			ctrl2 := slot.ctrl.Load()
			if ctrl != ctrl2 {
				probe--
				continue
			}
			if k != *key {
				continue
			}
			busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}

			slot.val.WriteUnfenced(*val)
			slot.ctrl.Store(ofhtCtrlUpdate(busyCtrl))
			return ofhtStoreOK, prev, true
		}
	}
	return ofhtStoreOK, *new(V), false
}

func (m *OFHTMap[K, V]) deleteFrom(
	table *ofhtTable[K, V],
	key *K,
	h1v uintptr,
	h2v uint16,
	needValue bool,
) (ofhtStoreStatus, V, bool) {
	// var spins int
	start := ofhtStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, *new(V), false
		}
		switch ofhtCtrlState(ctrl) {
		case ofhtStateEmpty:
			return ofhtStoreOK, *new(V), false
		case ofhtStateBusy:
			// delay(&spins)
			probe--
			continue
		case ofhtStateFull:
			if ofhtCtrlH2(ctrl) != h2v {
				continue
			}
			k := slot.key.ReadUnfenced()
			ctrl2 := slot.ctrl.Load()
			if ctrl != ctrl2 {
				// delay(&spins)
				probe--
				continue
			}
			if k != *key {
				continue
			}
			busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}

			var prev V
			if needValue {
				prev = slot.val.ReadUnfenced()
			}

			slot.key.WriteUnfenced(*new(K)) // Clear key/value for GC
			slot.val.WriteUnfenced(*new(V))
			slot.ctrl.Store(ofhtCtrlDelete(busyCtrl))
			m.size.Add(^uintptr(0))
			return ofhtStoreOK, prev, true
		}
	}
	return ofhtStoreOK, *new(V), false
}

func (m *OFHTMap[K, V]) compareAndSwapIn(
	table *ofhtTable[K, V],
	key *K,
	old *V,
	newVal *V,
	h1v uintptr,
	h2v uint16,
) (ofhtStoreStatus, bool) {
	// var spins int
	start := ofhtStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, false
		}
		switch ofhtCtrlState(ctrl) {
		case ofhtStateEmpty:
			return ofhtStoreOK, false
		case ofhtStateBusy:
			// delay(&spins)
			probe--
			continue
		case ofhtStateFull:
			if ofhtCtrlH2(ctrl) != h2v {
				continue
			}
			k := slot.key.ReadUnfenced()
			v := slot.val.ReadUnfenced()
			ctrl2 := slot.ctrl.Load()
			if ctrl != ctrl2 {
				// delay(&spins)
				probe--
				continue
			}
			if k != *key {
				continue
			}
			if m.valEqual == nil {
				panic("cc: value is not comparable; use WithValueEqual")
			}
			if !m.valEqual(noescape(unsafe.Pointer(&v)), noescape(unsafe.Pointer(old))) {
				return ofhtStoreOK, false
			}
			busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}

			slot.val.WriteUnfenced(*newVal)
			slot.ctrl.Store(ofhtCtrlUpdate(busyCtrl))
			return ofhtStoreOK, true
		}
	}
	return ofhtStoreOK, false
}

func (m *OFHTMap[K, V]) compareAndDeleteIn(
	table *ofhtTable[K, V],
	key *K,
	old *V,
	h1v uintptr,
	h2v uint16,
) (ofhtStoreStatus, bool) {
	// var spins int
	start := ofhtStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, false
		}
		switch ofhtCtrlState(ctrl) {
		case ofhtStateEmpty:
			return ofhtStoreOK, false
		case ofhtStateBusy:
			// delay(&spins)
			probe--
			continue
		case ofhtStateFull:
			if ofhtCtrlH2(ctrl) != h2v {
				continue
			}
			k := slot.key.ReadUnfenced()
			v := slot.val.ReadUnfenced()
			ctrl2 := slot.ctrl.Load()
			if ctrl != ctrl2 {
				// delay(&spins)
				probe--
				continue
			}
			if k != *key {
				continue
			}
			if m.valEqual == nil {
				panic("cc: value is not comparable; use WithValueEqual")
			}
			if !m.valEqual(noescape(unsafe.Pointer(&v)), noescape(unsafe.Pointer(old))) {
				return ofhtStoreOK, false
			}
			busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}

			slot.key.WriteUnfenced(*new(K)) // Clear key/value for GC
			slot.val.WriteUnfenced(*new(V))
			slot.ctrl.Store(ofhtCtrlDelete(busyCtrl))
			m.size.Add(^uintptr(0))
			return ofhtStoreOK, true
		}
	}
	return ofhtStoreOK, false
}

func (m *OFHTMap[K, V]) growIfNeeded(table *ofhtTable[K, V]) {
	// Fast path: Only do the full size check periodically based on local counter.
	// Since PLocalCounter is heavily sharded, getting the local value is cheap,
	// but it still involves a function call and runtime logic.
	localSize := int(m.size.Get().Load())
	if localSize&ofhtGrowCheckMask == 0 {
		if localSize >= table.stripeCap {
			if m.size.Value() >= table.growCap {
				m.tryGrow(table, (table.mask+1)<<1)
			}
		}
	}
}

func (m *OFHTMap[K, V]) tryGrow(old *ofhtTable[K, V], newLen uintptr) {
	if m.table.Load() != old {
		return
	}

	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			slotLen := old.mask + 1
			if newLen <= slotLen {
				newLen = slotLen << 1
			}
			newLen = nextPowOf2(max(newLen, m.minLen))

			newTable := newOFHTTable[K, V](newLen)
			old.nextTable.Store(newTable)
			next = newTable
		} else {
			for {
				next = old.nextTable.Load()
				if next != nil {
					break
				}
				runtime.Gosched()
			}
		}
	}

	slotLen := old.mask + 1
	cpus := maxProcs()
	chunks := uint32(calcParallelism(slotLen, cpus*resizeOverPartition))
	chunkSz := uintptr(max(1, slotLen>>bits.TrailingZeros32(chunks)))

	// Phase 1: Cooperative freezing.
	// All threads must help freeze the old table before anyone can safely copy.
	// This prevents open-addressing probe crossing boundaries.
	for {
		chunk := old.freezeIdx.Add(1) - 1
		if chunk >= chunks {
			break
		}
		start := uintptr(chunk) * chunkSz
		end := min(start+chunkSz, slotLen)
		for i := start; i < end; i++ {
			slot := old.slots.At(i)
			for {
				ctrl := slot.ctrl.Load()
				if ctrl&ofhtFrozen != 0 {
					break
				}
				if ofhtCtrlState(ctrl) == ofhtStateBusy {
					continue
				}
				if slot.ctrl.CompareAndSwap(ctrl, ctrl|ofhtFrozen) {
					break
				}
			}
		}
		old.frozenDone.Add(1)
	}

	// Wait for all threads to finish freezing.
	// We use copyIdx2 for the actual copying phase.
	for old.frozenDone.Load() < chunks {
		runtime.Gosched()
	}

	// Phase 2: Cooperative copying.
	for {
		chunk := old.copyIdx.Add(1) - 1
		if chunk >= chunks {
			break
		}
		start := uintptr(chunk) * chunkSz
		end := min(start+chunkSz, slotLen)
		for i := start; i < end; i++ {
			slot := old.slots.At(i)
			ctrl := slot.ctrl.Load()
			if ofhtCtrlState(ctrl) == ofhtStateFull {
				k := slot.key.ReadUnfenced()
				v := slot.val.ReadUnfenced()
				h1v, h2v := m.hashKey(noEscape(&k))

				// Inline copyEntry to avoid function call overhead and
				// keep the copying logic close to the source.
				destStart := ofhtStart(h1v, next.mask)
				for probe := uintptr(0); probe <= next.mask; probe++ {
					destSlot := next.slots.At((destStart + probe) & next.mask)
					destCtrl := destSlot.ctrl.Load()
					if ofhtCtrlState(destCtrl) != ofhtStateEmpty {
						continue
					}

					fullCtrl := ofhtCtrlInsert(destCtrl, h2v)
					if !destSlot.ctrl.CompareAndSwap(destCtrl, fullCtrl) {
						probe--
						continue
					}

					destSlot.key.WriteUnfenced(k)
					destSlot.val.WriteUnfenced(v)
					break // Successfully copied this entry
				}
			}
		}
		old.copyDone.Add(1)
	}

	for old.copyDone.Load() < chunks {
		runtime.Gosched()
	}

	m.table.CompareAndSwap(old, next)
}

func (m *OFHTMap[K, V]) afterFrozenTable(old *ofhtTable[K, V]) *ofhtTable[K, V] {
	for {
		table := m.table.Load()
		if table != old {
			return table
		}
		m.tryGrow(old, (old.mask+1)<<1)
	}
}

func newOFHTTable[K comparable, V any](slotLen uintptr) *ofhtTable[K, V] {
	slotLen = nextPowOf2(max(slotLen, ofhtMinSlots))
	growCap := uintptr(float64(slotLen) * ofhtLoadFactor)
	roundedSizeLen := nextPowOf2(maxProcs())
	return &ofhtTable[K, V]{
		slots:     makeUnsafeSlice[ofhtSlot[K, V]](slotLen),
		mask:      slotLen - 1,
		stripeCap: int(growCap >> bits.TrailingZeros32(uint32(roundedSizeLen))),
		growCap:   growCap,
	}
}

func ofhtCalcSlotLen(capacity uintptr) uintptr {
	if capacity == 0 {
		return ofhtMinSlots
	}
	const invLoadFactor = 1 / ofhtLoadFactor
	need := uintptr(float64(capacity+1) * invLoadFactor)
	return nextPowOf2(max(need, ofhtMinSlots))
}

//go:nosplit
func ofhtStart(h1v, mask uintptr) uintptr {
	return h1v & mask
}

//go:nosplit
func ofhtCtrlState(ctrl uint64) uint64 {
	return ctrl & ofhtStateMask
}

//go:nosplit
func ofhtCtrlH2(ctrl uint64) uint16 {
	return uint16(ctrl >> ofhtH2Shift)
}

//go:nosplit
func ofhtCtrlInsert(busyCtrl uint64, h2v uint16) uint64 {
	c := (busyCtrl &^ ofhtStateMask) | ofhtStateFull
	c = (c &^ (ofhtH2Mask << ofhtH2Shift)) | (uint64(h2v) << ofhtH2Shift)
	return c + ofhtSeqInc
}

//go:nosplit
func ofhtCtrlUpdate(busyCtrl uint64) uint64 {
	return (busyCtrl+ofhtSeqInc)&^ofhtStateMask | ofhtStateFull
}

//go:nosplit
func ofhtCtrlDelete(busyCtrl uint64) uint64 {
	return (busyCtrl+ofhtSeqInc)&^ofhtStateMask | ofhtStateDeleted
}
