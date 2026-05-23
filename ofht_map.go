//go:build !race

package cc

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/llxisdsh/cc/internal/opt"
)

const (
	ofhtEnableIntKey         = true
	ofhtUseRawIntHash        = false
	ofhtEnableDedupVal       = true
	ofhtEnableAggressiveGrow = true
	ofhtEnableStoreInGrow    = false
)

const (
	ofhtMinSlots = 128
	// ofhtLoadFactor must be a multiple of 1/8, such as 0.5, 0.625, 0.75, 0.875, etc.
	ofhtLoadFactor = 0.625

	// ofhtMaxProbeThreshold is the threshold of linear probing depth.
	// If a store operation probes more than this many slots without success,
	// it will eagerly trigger a resize even if the table is not fully loaded.
	ofhtMaxProbeThreshold = 1024

	// ofhtGrowCheckMask is used as a bitwise AND mask to sample the local size counter.
	// This reduces the overhead of checking the global size on every insertion.
	// It MUST be strictly smaller than the initial grow threshold.
	// If this mask is too large, the table could fill up
	// or hit the probe threshold before sampling the global size in highly
	// concurrent cold starts.
	ofhtGrowCheckMask = ofhtMinSlots/8 - 1 // Checks every 8th local insert
)

// OFHTMap is an experimental optimistic flat hash table.
//
// It uses open addressing with linear probing. Each table slot stores the key
// and value inline, plus a control word containing the slot state, a 32-bit
// hash fragment, a version counter, and a frozen bit used during resize.
//
// Concurrency model:
//   - Loads read a slot optimistically: control word, key/value, then control
//     word again. If the control word changed or the slot is busy, the load
//     retries that slot.
//   - Writes reserve a slot by CASing its control word to busy, update the
//     inline key/value fields, then publish a full or deleted control word.
//   - Resize allocates a next table, cooperatively freezes old slots, copies
//     frozen full slots, waits for all resize chunks to finish, then publishes
//     the new table.
//
// OFHTMap avoids root-bucket locks and per-entry heap allocation, but updates
// to a slot are not single-instruction atomic because generic K/V values are
// stored inline.
//
// OFHTMap is zero-value ready, but is intentionally excluded from race builds.
type OFHTMap[K comparable, V any] struct {
	_         noCopy
	table     atomic.Pointer[ofhtTable[K, V]]
	initState atomic.Uint32
	intKey    bool
	seed      uintptr
	keyHash   HashFunc
	valEqual  EqualFunc
	minLen    uintptr
	size      PLocalCounterN
}

type ofhtTable[K comparable, V any] struct {
	slots      unsafeSlice[ofhtSlot[K, V]]
	mask       uintptr
	stripeCap  int
	growCap    int
	chunkSz    uintptr
	chunks     uint32
	allocating atomic.Uint32 // 0: no one is allocating, 1: allocating
	copyIdx    atomic.Uint32 // Next chunk index for cooperative resize
	copyDone   atomic.Uint32 // Number of resize chunks completed
	nextTable  atomic.Pointer[ofhtTable[K, V]]
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
	ofhtH2Mask  = uint64(0xFFFFFFFF)

	// ctrl layout:
	//   bits 0..1:   slot state
	//   bits 2..33:  32-bit hash fragment
	//   bits 34..62: sequence, bumped on every slot transition
	//   bit 63:      frozen during resize
	// The sequence is part of the CAS expected ctrl word, so stale CAS
	// attempts cannot succeed after a delete/revive/update cycle.
	ofhtSeqShift = 34
	ofhtSeqInc   = uint64(1) << ofhtSeqShift

	ofhtFrozen = uint64(1) << 63
)

type ofhtStoreStatus uint8

const (
	ofhtStoreOK ofhtStoreStatus = iota
	ofhtStoreFrozen
	ofhtStoreFull
)

const (
	// occupied tracks physical slots that are no longer Empty.
	ofhtSlotOccupied int = iota
	// inserted/deleted are logical event counters; Size is inserted - deleted.
	// These P-local counters are not a transactional snapshot. Resize may use
	// them as pressure hints, but slot CAS state is the source of correctness.
	ofhtSlotInserted
	ofhtSlotDeleted
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
	// Inline hashKey()
	var h uint32
	if ofhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = ofhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	// var spins int
	start := ofhtStart(h, table.mask)
	limit := min(table.mask, uintptr(ofhtMaxProbeThreshold))
	for probe := uintptr(0); probe <= limit; probe++ {
		// Eagerly trigger a resize if the probe sequence is too long,
		// preventing severe performance degradation due to clustering.
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		state := ofhtCtrlState(ctrl)
		if state == ofhtStateFull {
			if ofhtCtrlH2(ctrl) != h {
				continue
			}
			k := slot.key.ReadUnfenced()
			v := slot.val.ReadUnfenced()
			ctrl2 := slot.ctrl.Load()
			if ctrl == ctrl2 {
				if k == key {
					return v, true
				}
			} else {
				// Slot was concurrently modified, retry this slot
				// delay(&spins)
				probe--
				continue
			}
		} else if state == ofhtStateEmpty {
			return *new(V), false
		} else if state == ofhtStateBusy {
			// delay(&spins)
			probe--
			continue
		}
	}
	return *new(V), false
}

// Store sets the value for a key.
func (m *OFHTMap[K, V]) Store(key K, value V) {
	table := m.ensureTable()
	// Inline hashKey()
	var h uint32
	if ofhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = ofhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, _, _ := m.storeInto(table, noEscape(&key), noEscape(&value), h, false)
		switch status {
		case ofhtStoreOK:
			m.resizeIfNeeded(table)
			return
		case ofhtStoreFrozen:
			table = m.afterFrozenTable(table)
		case ofhtStoreFull:
			occupied := int(m.size.Value(ofhtSlotOccupied))
			table = m.tryResize(table, occupied)
		}
	}
}

// LoadOrStore returns the existing value for the key if present. Otherwise it
// stores and returns the given value.
func (m *OFHTMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := m.ensureTable()
	// Inline hashKey()
	var h uint32
	if ofhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = ofhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, actual, loaded := m.storeInto(table, noEscape(&key), noEscape(&value), h, true)
		switch status {
		case ofhtStoreOK:
			if !loaded {
				m.resizeIfNeeded(table)
			}
			return actual, loaded
		case ofhtStoreFrozen:
			table = m.afterFrozenTable(table)
		case ofhtStoreFull:
			occupied := int(m.size.Value(ofhtSlotOccupied))
			table = m.tryResize(table, occupied)
		}
	}
}

// LoadAndUpdate retrieves the value for a key and updates it if the key exists.
func (m *OFHTMap[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	// Inline hashKey()
	var h uint32
	if ofhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = ofhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, prev, loaded := m.loadAndUpdateIn(table, noEscape(&key), noEscape(&value), h)
		switch status {
		case ofhtStoreOK:
			return prev, loaded
		case ofhtStoreFrozen:
			table = m.afterFrozenTable(table)
		case ofhtStoreFull:
			occupied := int(m.size.Value(ofhtSlotOccupied))
			table = m.tryResize(table, occupied)
		}
	}
}

// Delete removes the value for a key.
func (m *OFHTMap[K, V]) Delete(key K) {
	table := m.table.Load()
	if table == nil {
		return
	}
	// Inline hashKey()
	var h uint32
	if ofhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = ofhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, _, _ := m.deleteFrom(table, noEscape(&key), h, false)
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
	// Inline hashKey()
	var h uint32
	if ofhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = ofhtIntHash(hash)
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, prev, loaded := m.deleteFrom(table, noEscape(&key), h, true)
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
	// Inline hashKey()
	var h uint32
	if ofhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = ofhtIntHash(hash)
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
	// Inline hashKey()
	var h uint32
	if ofhtEnableIntKey && m.intKey {
		hash := intHash[K](noescape(unsafe.Pointer(&key)))
		h = ofhtIntHash(hash)
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
		if status == ofhtStoreOK {
			return deleted
		}
		table = m.afterFrozenTable(table)
	}
}

// Range iterates weakly over the table pointer observed at entry.
// It does not help an in-flight resize and may miss entries inserted into a
// next table after this call starts.
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
	table := m.table.Load()
	if table == nil {
		return 0
	}
	inserted := int(m.size.Value(ofhtSlotInserted))
	deleted := int(m.size.Value(ofhtSlotDeleted))
	if inserted <= deleted {
		return 0
	}
	return inserted - deleted
}

// Clear clears all key-value pairs from the map.
func (m *OFHTMap[K, V]) Clear() {
	table := m.table.Load()
	if table == nil {
		return
	}
	m.table.Store(newOFHTTable[K, V](m.minLen))
	m.size.Clear()
}

func (m *OFHTMap[K, V]) ensureTable() *ofhtTable[K, V] {
	table := m.table.Load()
	if table != nil {
		return table
	}
	return m.slowInit()
}

//go:noinline
func (m *OFHTMap[K, V]) slowInit() *ofhtTable[K, V] {
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

func (m *OFHTMap[K, V]) storeInto(
	table *ofhtTable[K, V],
	key *K,
	val *V,
	h uint32,
	onlyIfAbsent bool,
) (ofhtStoreStatus, V, bool) {
	// var spins int
	start := ofhtStart(h, table.mask)
	limit := min(table.mask, uintptr(ofhtMaxProbeThreshold))
	for probe := uintptr(0); probe <= limit; probe++ {
		// Eagerly trigger a resize if the probe sequence is too long,
		// preventing severe performance degradation due to clustering.
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, *new(V), false
		}
		state := ofhtCtrlState(ctrl)
		if state == ofhtStateFull {
			if ofhtCtrlH2(ctrl) != h {
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
		} else if state == ofhtStateDeleted {
			// Only the same key may revive its own tombstone. Reusing an
			// arbitrary Deleted slot is unsafe: another goroutine may have
			// inserted the same key earlier in the probe sequence meanwhile.
			if ofhtCtrlH2(ctrl) != h {
				continue
			}
			k := slot.key.ReadUnfenced()
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
			slot.ctrl.Store(ofhtCtrlInsert(busyCtrl, h))
			m.size.Add(ofhtSlotInserted, 1)
			return ofhtStoreOK, *val, false
		} else if state == ofhtStateBusy {
			// delay(&spins)
			probe--
			continue
		} else if state == ofhtStateEmpty {
			busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}
			slot.key.WriteUnfenced(*key)
			slot.val.WriteUnfenced(*val)
			slot.ctrl.Store(ofhtCtrlInsert(busyCtrl, h))
			m.size.Add2(ofhtSlotOccupied, 1, ofhtSlotInserted, 1)
			return ofhtStoreOK, *val, false
		}
	}
	return ofhtStoreFull, *new(V), false
}

func (m *OFHTMap[K, V]) loadAndUpdateIn(
	table *ofhtTable[K, V],
	key *K,
	val *V,
	h uint32,
) (ofhtStoreStatus, V, bool) {
	start := ofhtStart(h, table.mask)
	limit := min(table.mask, uintptr(ofhtMaxProbeThreshold))
	for probe := uintptr(0); probe <= limit; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, *new(V), false
		}
		state := ofhtCtrlState(ctrl)
		if state == ofhtStateFull {
			if ofhtCtrlH2(ctrl) != h {
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
			if ofhtEnableDedupVal {
				if m.valEqual != nil && m.valEqual(noescape(unsafe.Pointer(&prev)), noescape(unsafe.Pointer(val))) {
					return ofhtStoreOK, prev, true
				}
			}
			busyCtrl := (ctrl &^ ofhtStateMask) | ofhtStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}

			slot.val.WriteUnfenced(*val)
			slot.ctrl.Store(ofhtCtrlUpdate(busyCtrl))
			return ofhtStoreOK, prev, true
		} else if state == ofhtStateEmpty {
			return ofhtStoreOK, *new(V), false
		} else if state == ofhtStateBusy {
			probe--
			continue
		}
	}
	return ofhtStoreOK, *new(V), false
}

func (m *OFHTMap[K, V]) deleteFrom(
	table *ofhtTable[K, V],
	key *K,
	h uint32,
	needValue bool,
) (ofhtStoreStatus, V, bool) {
	// var spins int
	start := ofhtStart(h, table.mask)
	limit := min(table.mask, uintptr(ofhtMaxProbeThreshold))
	for probe := uintptr(0); probe <= limit; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, *new(V), false
		}
		state := ofhtCtrlState(ctrl)
		if state == ofhtStateFull {
			if ofhtCtrlH2(ctrl) != h {
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

			// Keep key attached to the tombstone. The key is needed to allow
			// same-key revival without permitting arbitrary tombstone reuse.
			slot.val.WriteUnfenced(*new(V))
			slot.ctrl.Store(ofhtCtrlDelete(busyCtrl))
			m.size.Add(ofhtSlotDeleted, 1)
			return ofhtStoreOK, prev, true
		} else if state == ofhtStateEmpty {
			return ofhtStoreOK, *new(V), false
		} else if state == ofhtStateBusy {
			// delay(&spins)
			probe--
			continue
		}
	}
	return ofhtStoreOK, *new(V), false
}

func (m *OFHTMap[K, V]) compareAndSwapIn(
	table *ofhtTable[K, V],
	key *K,
	old *V,
	newVal *V,
	h uint32,
) (ofhtStoreStatus, bool) {
	// var spins int
	start := ofhtStart(h, table.mask)
	limit := min(table.mask, uintptr(ofhtMaxProbeThreshold))
	for probe := uintptr(0); probe <= limit; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, false
		}
		state := ofhtCtrlState(ctrl)
		if state == ofhtStateFull {
			if ofhtCtrlH2(ctrl) != h {
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
		} else if state == ofhtStateEmpty {
			return ofhtStoreOK, false
		} else if state == ofhtStateBusy {
			// delay(&spins)
			probe--
			continue
		}
	}
	return ofhtStoreOK, false
}

func (m *OFHTMap[K, V]) compareAndDeleteIn(
	table *ofhtTable[K, V],
	key *K,
	old *V,
	h uint32,
) (ofhtStoreStatus, bool) {
	// var spins int
	start := ofhtStart(h, table.mask)
	limit := min(table.mask, uintptr(ofhtMaxProbeThreshold))
	for probe := uintptr(0); probe <= limit; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&ofhtFrozen != 0 {
			return ofhtStoreFrozen, false
		}
		state := ofhtCtrlState(ctrl)
		if state == ofhtStateFull {
			if ofhtCtrlH2(ctrl) != h {
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

			// Keep key attached to the tombstone for the same reason as
			// deleteFrom: only the original key may revive this slot.
			slot.val.WriteUnfenced(*new(V))
			slot.ctrl.Store(ofhtCtrlDelete(busyCtrl))
			m.size.Add(ofhtSlotDeleted, 1)
			return ofhtStoreOK, true
		} else if state == ofhtStateEmpty {
			return ofhtStoreOK, false
		} else if state == ofhtStateBusy {
			// delay(&spins)
			probe--
			continue
		}
	}
	return ofhtStoreOK, false
}

func (m *OFHTMap[K, V]) resizeIfNeeded(table *ofhtTable[K, V]) {
	localSize := int(m.size.Get(ofhtSlotOccupied))
	if localSize&ofhtGrowCheckMask != 0 {
		return
	}
	if localSize < table.stripeCap {
		return
	}
	occupied := m.size.Value(ofhtSlotOccupied)
	if int(occupied) >= table.growCap {
		m.tryResize(table, int(occupied))
	}
}

func (m *OFHTMap[K, V]) resizeShouldGrow(occupied, tombstones int) bool {
	if occupied < 2 {
		return true
	}
	half := occupied >> 1
	return tombstones < half
}

func (m *OFHTMap[K, V]) tryResize(old *ofhtTable[K, V], occupied int) *ofhtTable[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}

	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			inserted := int(m.size.Value(ofhtSlotInserted))
			deleted := int(m.size.Value(ofhtSlotDeleted))
			live := max(inserted-deleted, 0)
			// occupied/inserted/deleted are read independently, so occupied can
			// briefly lag behind live. Treat that as zero tombstone pressure
			// rather than clamping live and corrupting Size after resize.
			effectiveOccupied := max(live, occupied)
			tombstones := effectiveOccupied - live
			isGrow := m.resizeShouldGrow(effectiveOccupied, tombstones)
			newTable := newOFHTTable[K, V](m.nextResizeSlotLen(old, isGrow, live))
			old.nextTable.Store(newTable)
			next = newTable
		} else {
			if ofhtEnableStoreInGrow {
				// When this switch is on, writers may keep retrying store paths while
				// resize is still allocating/cooperating. In OFHT, busy-slot handshakes
				// (ofhtStateBusy) can amplify that contention; turning this switch off
				// usually makes tryResize progress more smoothly.
				// Frozen slots will force helping.
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

func (m *OFHTMap[K, V]) helpResize(old *ofhtTable[K, V]) *ofhtTable[K, V] {
	if table := m.table.Load(); table != old {
		return table
	}

	next := old.nextTable.Load()
	if next == nil {
		return old
	}
	return m.helpResizeInto(old, next)
}

func (m *OFHTMap[K, V]) helpResizeInto(old, next *ofhtTable[K, V]) *ofhtTable[K, V] {
	slotLen := old.mask + 1
	chunks, chunkSz := old.chunks, old.chunkSz
	probeLimit := min(next.mask, uintptr(ofhtMaxProbeThreshold))

	// Cooperative resize.
	// Each slot is frozen before it is copied. This removes the old global
	// freeze barrier while preserving the key rule: a copied slot can no longer
	// be modified in the old table.
	for {
		chunk := old.copyIdx.Add(1) - 1
		if chunk >= chunks {
			for {
				table := m.table.Load()
				if table != old {
					return table
				}
				runtime.Gosched()
			}
		}
		start := uintptr(chunk) * chunkSz
		end := min(start+chunkSz, slotLen)
		for i := start; i < end; i++ {
			slot := old.slots.At(i)
			var ctrl uint64
			for {
				ctrl = slot.ctrl.Load()
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

			if ofhtCtrlState(ctrl) == ofhtStateFull {
				k := slot.key.ReadUnfenced()
				v := slot.val.ReadUnfenced()
				h := ofhtCtrlH2(ctrl)
				// Inline copyEntry to avoid function call overhead and
				// keep the copying logic close to the source.
				destStart := ofhtStart(h, next.mask)
				probe := uintptr(0)
				for ; probe <= probeLimit; probe++ {
					destSlot := next.slots.At((destStart + probe) & next.mask)
					destCtrl := destSlot.ctrl.Load()
					if ofhtCtrlState(destCtrl) != ofhtStateEmpty {
						continue
					}

					fullCtrl := ofhtCtrlInsert(destCtrl, h)
					if !destSlot.ctrl.CompareAndSwap(destCtrl, fullCtrl) {
						probe--
						continue
					}

					destSlot.key.WriteUnfenced(k)
					destSlot.val.WriteUnfenced(v)
					break // Successfully copied this entry
				}
				if probe > probeLimit {
					panic("cc: OFHTMap grow exceeded max probe threshold")
				}
			}
		}
		if old.copyDone.Add(1) == chunks {
			// Reset counters to describe the compacted next table. The three
			// counters are not reset atomically; preserve logical live count
			// from inserted-deleted and do not constrain it by occupied.
			m.size.Reset(ofhtSlotOccupied)
			inserted := int(m.size.Reset(ofhtSlotInserted))
			deleted := int(m.size.Reset(ofhtSlotDeleted))
			live := max(inserted-deleted, 0)
			if live > 0 {
				m.size.Add2(ofhtSlotOccupied, uintptr(live), ofhtSlotInserted, uintptr(live))
			}
			m.table.CompareAndSwap(old, next)
			return next
		}
	}
}

func (m *OFHTMap[K, V]) nextResizeSlotLen(old *ofhtTable[K, V], isGrow bool, live int) uintptr {
	slotLen := old.mask + 1
	nextLen := slotLen
	if isGrow {
		nextLen <<= 1
	}
	if !ofhtEnableAggressiveGrow {
		return nextLen
	}

	// Resize does not stop all writers immediately; goroutines that already
	// hold old can keep inserting into slots that have not been frozen yet.
	// When that concurrent window makes old much denser than the normal grow
	// threshold, grow for roughly another table worth of inserts instead of
	// forcing a near-immediate second resize.
	if live > 0 {
		liveSlots := min(uintptr(live), slotLen)
		nextLen = max(nextLen, ofhtCalcSlotLen(liveSlots<<1))
	}
	if ofhtEnableIntKey && ofhtUseRawIntHash && m.intKey {
		// Raw integer hashes preserve sequential-insert locality, but during
		// concurrent no-pre-size growth, far-apart ordered ranges can alias on
		// the low bits of a small power-of-two table. That creates one dense
		// run and can make resize copying exceed ofhtMaxProbeThreshold even
		// when the destination table has plenty of empty slots elsewhere.
		//
		// Use the observed hash span as an address-space hint only for int keys:
		// dense spans get enough slots to avoid low-bit folding, while sparse
		// spans are capped in ofhtHashSpanGrowSlotLen so outliers cannot force
		// an unbounded allocation.
		if span, count := ofhtObservedHashSpan(old); span > 0 {
			if target := ofhtHashSpanGrowSlotLen(span, count, slotLen); target > 0 {
				nextLen = max(nextLen, target)
			}
		}
	}
	return nextLen
}

func ofhtObservedHashSpan[K comparable, V any](table *ofhtTable[K, V]) (uintptr, uintptr) {
	slotLen := table.mask + 1
	var minH, maxH uint32
	seen := false
	count := uintptr(0)
	for i := range slotLen {
		ctrl := table.slots.At(i).ctrl.Load()
		if ofhtCtrlState(ctrl) != ofhtStateFull {
			continue
		}
		h := ofhtCtrlH2(ctrl)
		count++
		if !seen {
			minH, maxH, seen = h, h, true
			continue
		}
		if h < minH {
			minH = h
		}
		if h > maxH {
			maxH = h
		}
	}
	if !seen {
		return 0, 0
	}
	return uintptr(maxH) - uintptr(minH) + 1, count
}

func ofhtHashSpanGrowSlotLen(span, count, slotLen uintptr) uintptr {
	// Integer-key span growth is a guard for low-bit aliasing during concurrent
	// ordered inserts. Dense spans use the exact observed range; sparse spans
	// are capped so a few outliers cannot force an enormous table.
	const (
		denseHashSpanRatio  = 8
		sparseHashSpanRatio = 32
		tableHashSpanRatio  = 16
	)
	if span == 0 || count == 0 {
		return 0
	}
	target := ofhtCalcSlotLen(span)
	if span <= count*denseHashSpanRatio {
		return target
	}

	limit := max(
		ofhtCalcSlotLen(count*sparseHashSpanRatio),
		slotLen*tableHashSpanRatio,
	)
	return min(target, limit)
}

func (m *OFHTMap[K, V]) afterFrozenTable(old *ofhtTable[K, V]) *ofhtTable[K, V] {
	for {
		if table := m.helpResize(old); table != old {
			return table
		}
		runtime.Gosched()
	}
}

func newOFHTTable[K comparable, V any](slotLen uintptr) *ofhtTable[K, V] {
	slotLen = nextPowOf2(max(slotLen, ofhtMinSlots))
	growCap := int(float64(slotLen) * ofhtLoadFactor)
	cpus := maxProcs()
	roundedSizeLen := nextPowOf2(cpus)
	chunks, chunkSz := ofhtResizeChunks(slotLen, cpus)
	return &ofhtTable[K, V]{
		slots:     makeUnsafeSlice[ofhtSlot[K, V]](slotLen),
		mask:      slotLen - 1,
		stripeCap: int(growCap >> bits.TrailingZeros32(uint32(roundedSizeLen))),
		growCap:   growCap,
		chunks:    chunks,
		chunkSz:   chunkSz,
	}
}

//go:nosplit
func ofhtCalcSlotLen(capacity uintptr) uintptr {
	if capacity == 0 {
		return ofhtMinSlots
	}
	const invLoadFactor = 1 / ofhtLoadFactor
	need := uintptr(float64(capacity+1) * invLoadFactor)
	return nextPowOf2(max(need, ofhtMinSlots))
}

//go:nosplit
func ofhtResizeChunks(slotLen, cpus uintptr) (chunks uint32, chunkSz uintptr) {
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
func ofhtStart(h1v uint32, mask uintptr) uintptr {
	return uintptr(h1v) & mask
}

//go:nosplit
func ofhtCtrlState(ctrl uint64) uint64 {
	return ctrl & ofhtStateMask
}

//go:nosplit
func ofhtCtrlH2(ctrl uint64) uint32 {
	return uint32(ctrl >> ofhtH2Shift)
}

//go:nosplit
func ofhtCtrlInsert(busyCtrl uint64, h2v uint32) uint64 {
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

//go:nosplit
func ofhtIntHash(x uintptr) uint32 {
	if ofhtUseRawIntHash {
		return uint32(x ^ (x >> 32))
	}
	return uint32((uint64(x) * opt.HashPrime) >> 32)
}
