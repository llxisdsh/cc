package cc

import (
	"math/bits"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"unsafe"
)

// DHLTMap is an experimental DHLT-style h1v map.
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
type DHLTMap[K comparable, V any] struct {
	_ noCopy

	table    atomic.Pointer[dhltTable[K, V]]
	resizeMu sync.Mutex

	intKey   bool
	seed     uintptr
	keyHash  HashFunc
	valEqual EqualFunc
	minLen   uintptr
	size     PLocalCounter
}

type dhltTable[K comparable, V any] struct {
	slots     unsafeSlice[dhltSlot[K, V]]
	mask      uintptr
	stripeCap int
	growCap   uintptr
}

type dhltSlot[K comparable, V any] struct {
	ctrl atomic.Uint64
	key  SeqLockSlot[K]
	val  SeqLockSlot[V]
}

const (
	dhltGroupSlots = uintptr(8)
	dhltMinSlots   = uintptr(64)

	dhltStateMask    = uint64(0x3)
	dhltStateEmpty   = uint64(0)
	dhltStateFull    = uint64(1)
	dhltStateDeleted = uint64(2)
	dhltStateBusy    = uint64(3)

	dhltH2Shift = 2
	dhltH2Mask  = uint64(0xFFFF)

	dhltSeqShift = 18
	dhltSeqInc   = uint64(1) << dhltSeqShift

	dhltFrozen = uint64(1) << 63

	dhltGrowNumerator   = uintptr(3)
	dhltGrowDenominator = uintptr(4)
)

type dhltStoreStatus uint8

const (
	dhltStoreOK dhltStoreStatus = iota
	dhltStoreFrozen
	dhltStoreFull
	dhltStoreRetry
)

// NewDHLTMap creates an experimental DHLT-style map.
func NewDHLTMap[K comparable, V any](options ...func(*MapConfig)) *DHLTMap[K, V] {
	var cfg MapConfig
	for _, o := range options {
		o(noEscape(&cfg))
	}
	m := &DHLTMap[K, V]{}
	m.init(noEscape(&cfg))
	return m
}

func (m *DHLTMap[K, V]) init(cfg *MapConfig) {
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
	m.minLen = dhltCalcSlotLen(cfg.capacity)
	m.table.Store(newDHLTTable[K, V](m.minLen))
}

// Load retrieves the value for a key.
func (m *DHLTMap[K, V]) Load(key K) (value V, ok bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	return m.loadFrom(table, noEscape(&key), h1v, h2v)
}

// Store sets the value for a key.
func (m *DHLTMap[K, V]) Store(key K, value V) {
	table := m.ensureTable()
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, _, _ := m.storeInto(table, noEscape(&key), noEscape(&value), h1v, h2v, false)
		switch status {
		case dhltStoreOK:
			m.growIfNeeded(table)
			return
		case dhltStoreFrozen:
			table = m.afterFrozenTable(table)
		case dhltStoreFull:
			m.tryGrow(table, (table.mask+1)<<1)
			table = m.ensureTable()
		case dhltStoreRetry:
			table = m.ensureTable()
		}
	}
}

// LoadOrStore returns the existing value for the key if present. Otherwise it
// stores and returns the given value.
func (m *DHLTMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	table := m.ensureTable()
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, actual, loaded := m.storeInto(table, noEscape(&key), noEscape(&value), h1v, h2v, true)
		switch status {
		case dhltStoreOK:
			if !loaded {
				m.growIfNeeded(table)
			}
			return actual, loaded
		case dhltStoreFrozen:
			table = m.afterFrozenTable(table)
		case dhltStoreFull:
			m.tryGrow(table, (table.mask+1)<<1)
			table = m.ensureTable()
		case dhltStoreRetry:
			table = m.ensureTable()
		}
	}
}

// Delete removes the value for a key.
func (m *DHLTMap[K, V]) Delete(key K) {
	table := m.table.Load()
	if table == nil {
		return
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, _, _ := m.deleteFrom(table, noEscape(&key), h1v, h2v, false)
		if status == dhltStoreOK {
			return
		}
		table = m.afterFrozenTable(table)
	}
}

// LoadAndDelete retrieves the value for a key and deletes it from the map.
func (m *DHLTMap[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	table := m.table.Load()
	if table == nil {
		return *new(V), false
	}
	h1v, h2v := m.hashKey(noEscape(&key))
	for {
		status, prev, loaded := m.deleteFrom(table, noEscape(&key), h1v, h2v, true)
		if status == dhltStoreOK {
			return prev, loaded
		}
		table = m.afterFrozenTable(table)
	}
}

// CompareAndSwap atomically replaces an existing value with a new value.
func (m *DHLTMap[K, V]) CompareAndSwap(key K, old V, new V) bool {
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
		if status == dhltStoreOK {
			return swapped
		}
		table = m.afterFrozenTable(table)
	}
}

// CompareAndDelete atomically deletes an existing entry.
// If its value matches the expected value.
func (m *DHLTMap[K, V]) CompareAndDelete(key K, old V) bool {
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
		if status == dhltStoreOK {
			return deleted
		}
		table = m.afterFrozenTable(table)
	}
}

// Range iterates over a weakly consistent snapshot of the table.
func (m *DHLTMap[K, V]) Range(yield func(K, V) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}
	for i := uintptr(0); i <= table.mask; i++ {
		slot := table.slots.At(i)
	retry:
		for {
			ctrl := slot.ctrl.Load()
			if dhltCtrlState(ctrl) != dhltStateFull {
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
func (m *DHLTMap[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// Size returns the approximate number of entries in the map.
func (m *DHLTMap[K, V]) Size() int {
	return int(m.size.Value())
}

func (m *DHLTMap[K, V]) ensureTable() *dhltTable[K, V] {
	table := m.table.Load()
	if table != nil {
		return table
	}
	var cfg MapConfig
	m.resizeMu.Lock()
	if table = m.table.Load(); table == nil {
		m.init(noEscape(&cfg))
		table = m.table.Load()
	}
	m.resizeMu.Unlock()
	return table
}

//go:nosplit
func (m *DHLTMap[K, V]) hashKey(key *K) (uintptr, uint16) {
	if m.intKey {
		h1v := intHash[K](noescape(unsafe.Pointer(key)))
		return h1v, uint16(h1v ^ (h1v >> 16))
	}
	h1v := m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
	return h1v >> 16, uint16(h1v)
}

func (m *DHLTMap[K, V]) loadFrom(
	table *dhltTable[K, V],
	key *K,
	h1v uintptr,
	h2v uint16,
) (value V, ok bool) {
	// var spins int
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		switch dhltCtrlState(ctrl) {
		case dhltStateEmpty:
			return *new(V), false
		case dhltStateBusy:
			// delay(&spins)
			probe--
			continue
		case dhltStateFull:
			if dhltCtrlH2(ctrl) != h2v {
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

func (m *DHLTMap[K, V]) storeInto(
	table *dhltTable[K, V],
	key *K,
	val *V,
	h1v uintptr,
	h2v uint16,
	onlyIfAbsent bool,
) (dhltStoreStatus, V, bool) {
	var (
		// spins     int
		deleted   *dhltSlot[K, V]
		deletedC  uint64
		deletedOK bool
	)
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&dhltFrozen != 0 {
			return dhltStoreFrozen, *new(V), false
		}
		switch dhltCtrlState(ctrl) {
		case dhltStateBusy:
			// delay(&spins)
			probe--
			continue
		case dhltStateFull:
			if dhltCtrlH2(ctrl) != h2v {
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
				return dhltStoreOK, v, true
			}
			if m.valEqual != nil && m.valEqual(noescape(unsafe.Pointer(&v)), noescape(unsafe.Pointer(val))) {
				return dhltStoreOK, v, true
			}
			busyCtrl := (ctrl &^ dhltStateMask) | dhltStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}
			slot.val.WriteUnfenced(*val)
			slot.ctrl.Store(dhltCtrlUpdate(busyCtrl))
			return dhltStoreOK, *val, true
		case dhltStateDeleted:
			if !deletedOK {
				deleted, deletedC, deletedOK = slot, ctrl, true
			}
		case dhltStateEmpty:
			if deletedOK {
				status, rVal, loaded := m.claimSlot(deleted, deletedC, key, val, h2v)
				if status == dhltStoreRetry {
					return dhltStoreRetry, *new(V), false
				}
				return status, rVal, loaded
			}
			busyCtrl := (ctrl &^ dhltStateMask) | dhltStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}
			slot.key.WriteUnfenced(*key)
			slot.val.WriteUnfenced(*val)
			slot.ctrl.Store(dhltCtrlInsert(busyCtrl, h2v))
			m.size.Add(1)
			return dhltStoreOK, *val, false
		}
	}
	if deletedOK {
		return m.claimSlot(deleted, deletedC, key, val, h2v)
	}
	return dhltStoreFull, *new(V), false
}

func (m *DHLTMap[K, V]) claimSlot(
	slot *dhltSlot[K, V],
	ctrl uint64,
	key *K,
	val *V,
	h2v uint16,
) (dhltStoreStatus, V, bool) {
	if ctrl&dhltFrozen != 0 {
		return dhltStoreFrozen, *new(V), false
	}
	busyCtrl := (ctrl &^ dhltStateMask) | dhltStateBusy
	if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
		return dhltStoreRetry, *new(V), false
	}
	slot.key.WriteUnfenced(*key)
	slot.val.WriteUnfenced(*val)
	slot.ctrl.Store(dhltCtrlInsert(busyCtrl, h2v))
	m.size.Add(1)
	return dhltStoreOK, *val, false
}

func (m *DHLTMap[K, V]) deleteFrom(
	table *dhltTable[K, V],
	key *K,
	h1v uintptr,
	h2v uint16,
	needValue bool,
) (dhltStoreStatus, V, bool) {
	// var spins int
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&dhltFrozen != 0 {
			return dhltStoreFrozen, *new(V), false
		}
		switch dhltCtrlState(ctrl) {
		case dhltStateEmpty:
			return dhltStoreOK, *new(V), false
		case dhltStateBusy:
			// delay(&spins)
			probe--
			continue
		case dhltStateFull:
			if dhltCtrlH2(ctrl) != h2v {
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
			busyCtrl := (ctrl &^ dhltStateMask) | dhltStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}

			var prev V
			if needValue {
				prev = slot.val.ReadUnfenced()
			}

			slot.val.WriteUnfenced(*new(V)) // Clear value for GC
			slot.ctrl.Store(dhltCtrlDelete(busyCtrl))
			m.size.Add(^uintptr(0))
			return dhltStoreOK, prev, true
		}
	}
	return dhltStoreOK, *new(V), false
}

func (m *DHLTMap[K, V]) compareAndSwapIn(
	table *dhltTable[K, V],
	key *K,
	old *V,
	newVal *V,
	h1v uintptr,
	h2v uint16,
) (dhltStoreStatus, bool) {
	// var spins int
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&dhltFrozen != 0 {
			return dhltStoreFrozen, false
		}
		switch dhltCtrlState(ctrl) {
		case dhltStateEmpty:
			return dhltStoreOK, false
		case dhltStateBusy:
			// delay(&spins)
			probe--
			continue
		case dhltStateFull:
			if dhltCtrlH2(ctrl) != h2v {
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
				return dhltStoreOK, false
			}
			busyCtrl := (ctrl &^ dhltStateMask) | dhltStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}

			slot.val.WriteUnfenced(*newVal)
			slot.ctrl.Store(dhltCtrlUpdate(busyCtrl))
			return dhltStoreOK, true
		}
	}
	return dhltStoreOK, false
}

func (m *DHLTMap[K, V]) compareAndDeleteIn(
	table *dhltTable[K, V],
	key *K,
	old *V,
	h1v uintptr,
	h2v uint16,
) (dhltStoreStatus, bool) {
	// var spins int
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if ctrl&dhltFrozen != 0 {
			return dhltStoreFrozen, false
		}
		switch dhltCtrlState(ctrl) {
		case dhltStateEmpty:
			return dhltStoreOK, false
		case dhltStateBusy:
			// delay(&spins)
			probe--
			continue
		case dhltStateFull:
			if dhltCtrlH2(ctrl) != h2v {
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
				return dhltStoreOK, false
			}
			busyCtrl := (ctrl &^ dhltStateMask) | dhltStateBusy
			if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
				probe--
				continue
			}

			slot.val.WriteUnfenced(*new(V)) // Clear value for GC
			slot.ctrl.Store(dhltCtrlDelete(busyCtrl))
			m.size.Add(^uintptr(0))
			return dhltStoreOK, true
		}
	}
	return dhltStoreOK, false
}

func (m *DHLTMap[K, V]) growIfNeeded(table *dhltTable[K, V]) {
	if int(m.size.Get().Load()) >= table.stripeCap {
		if m.size.Value() >= table.growCap {
			m.tryGrow(table, (table.mask+1)<<1)
		}
	}
}

func (m *DHLTMap[K, V]) tryGrow(old *dhltTable[K, V], newLen uintptr) {
	m.resizeMu.Lock()
	defer m.resizeMu.Unlock()

	if m.table.Load() != old {
		return
	}
	slotLen := old.mask + 1
	if newLen <= slotLen {
		newLen = slotLen << 1
	}
	newLen = nextPowOf2(max(newLen, m.minLen))

	m.freezeTable(old)
	newTable := newDHLTTable[K, V](newLen)
	for i := uintptr(0); i <= old.mask; i++ {
		slot := old.slots.At(i)
		ctrl := slot.ctrl.Load()
		if dhltCtrlState(ctrl) != dhltStateFull {
			continue
		}
		k := slot.key.ReadUnfenced()
		v := slot.val.ReadUnfenced()
		h1v, h2v := m.hashKey(noEscape(&k))
		m.copyEntry(newTable, &k, &v, h1v, h2v)
	}
	m.table.Store(newTable)
}

func (m *DHLTMap[K, V]) freezeTable(table *dhltTable[K, V]) {
	// var spins int
	for i := uintptr(0); i <= table.mask; i++ {
		slot := table.slots.At(i)
		for {
			ctrl := slot.ctrl.Load()
			if ctrl&dhltFrozen != 0 {
				break
			}
			if dhltCtrlState(ctrl) == dhltStateBusy {
				// delay(&spins)
				continue
			}
			if slot.ctrl.CompareAndSwap(ctrl, ctrl|dhltFrozen) {
				break
			}
		}
	}
}

func (m *DHLTMap[K, V]) copyEntry(
	table *dhltTable[K, V],
	key *K,
	val *V,
	h1v uintptr,
	h2v uint16,
) {
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		slot := table.slots.At((start + probe) & table.mask)
		ctrl := slot.ctrl.Load()
		if dhltCtrlState(ctrl) != dhltStateEmpty {
			continue
		}
		busyCtrl := (ctrl &^ dhltStateMask) | dhltStateBusy
		if !slot.ctrl.CompareAndSwap(ctrl, busyCtrl) {
			probe--
			continue
		}
		slot.key.WriteUnfenced(*key)
		slot.val.WriteUnfenced(*val)
		slot.ctrl.Store(dhltCtrlInsert(busyCtrl, h2v))
		return
	}
	panic("cc: DHLTMap grow produced a full table")
}

func (m *DHLTMap[K, V]) afterFrozenTable(old *dhltTable[K, V]) *dhltTable[K, V] {
	for {
		table := m.table.Load()
		if table != old {
			return table
		}
		m.tryGrow(old, (old.mask+1)<<1)
	}
}

func newDHLTTable[K comparable, V any](slotLen uintptr) *dhltTable[K, V] {
	slotLen = nextPowOf2(max(slotLen, dhltMinSlots))
	if rem := slotLen & (dhltGroupSlots - 1); rem != 0 {
		slotLen += dhltGroupSlots - rem
		slotLen = nextPowOf2(slotLen)
	}
	growCap := (slotLen * dhltGrowNumerator) / dhltGrowDenominator
	roundedSizeLen := nextPowOf2(maxProcs())
	return &dhltTable[K, V]{
		slots:     makeUnsafeSlice[dhltSlot[K, V]](slotLen),
		mask:      slotLen - 1,
		stripeCap: int(growCap >> bits.TrailingZeros32(uint32(roundedSizeLen))),
		growCap:   growCap,
	}
}

func dhltCalcSlotLen(capacity uintptr) uintptr {
	if capacity == 0 {
		return dhltMinSlots
	}
	need := (capacity*dhltGrowDenominator + dhltGrowNumerator - 1) / dhltGrowNumerator
	return nextPowOf2(max(need, dhltMinSlots))
}

//go:nosplit
func dhltStart(h1v, mask uintptr) uintptr {
	return h1v & mask
}

//go:nosplit
func dhltCtrlState(ctrl uint64) uint64 {
	return ctrl & dhltStateMask
}

//go:nosplit
func dhltCtrlH2(ctrl uint64) uint16 {
	return uint16((ctrl >> dhltH2Shift) /*& 0xFFFF*/)
}

//go:nosplit
func dhltCtrlInsert(busyCtrl uint64, h2v uint16) uint64 {
	c := (busyCtrl &^ dhltStateMask) | dhltStateFull
	c = (c &^ (dhltH2Mask << dhltH2Shift)) | (uint64(h2v) << dhltH2Shift)
	return c + dhltSeqInc
}

//go:nosplit
func dhltCtrlUpdate(busyCtrl uint64) uint64 {
	return (busyCtrl+dhltSeqInc)&^dhltStateMask | dhltStateFull
}

//go:nosplit
func dhltCtrlDelete(busyCtrl uint64) uint64 {
	return (busyCtrl+dhltSeqInc)&^dhltStateMask | dhltStateDeleted
}
