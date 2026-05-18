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
	dwhtEnableIntKey         = true
	dwhtEnableDedupVal       = true
	dwhtEnableAggressiveGrow = true
)

const (
	dwhtMinSlots = 128
	// dwhtLoadFactor must be a multiple of 1/8, such as 0.5, 0.625, 0.75, 0.875, etc.
	dwhtLoadFactor = 0.625

	// dwhtMaxProbeThreshold is the threshold of linear probing depth.
	// If a store operation probes more than this many slots without success,
	// it will eagerly trigger a resize even if the table is not fully loaded.
	dwhtMaxProbeThreshold = 1024

	// dwhtGrowCheckMask is used as a bitwise AND mask to sample the local size counter.
	// This reduces the overhead of checking the global size on every insertion.
	// It MUST be strictly smaller than the initial grow threshold.
	// Since dwhtMinSlots is 64 and dwhtLoadFactor is 0.75, the first grow
	// happens at 96 entries. If this mask is too large, the table could fill up
	// or hit the probe threshold before sampling the global size in highly
	// concurrent cold starts.
	dwhtGrowCheckMask = dwhtMinSlots/8 - 1 // Checks every 8th local insert
)

// DWHTMap is an experimental double-word-CAS hash table.
//
// It uses open addressing with linear probing. Each table slot is two machine
// words: a control word and an entry pointer. The control word contains the
// slot state, a short hash fingerprint, a version counter, and a frozen bit
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
// Compared with OFHTMap, DWHTMap pays one heap object per live entry, but slot
// publication is atomic and readers never observe a busy inline-update state.
type DWHTMap[K comparable, V any] struct {
	_        noCopy
	table    atomic.Pointer[dwhtTable[K, V]]
	intKey   bool
	seed     uintptr
	keyHash  HashFunc
	valEqual EqualFunc
	minLen   uintptr
	size     PLocalCounter
}

type dwhtTable[K comparable, V any] struct {
	slotsBase  unsafe.Pointer
	mask       uintptr
	stripeCap  int
	growCap    int
	nextTable  atomic.Pointer[dwhtTable[K, V]]
	allocating atomic.Uint32  // 0: no one is allocating, 1: allocating
	copyIdx    atomic.Uint32  // Next chunk index for cooperative resize
	copyDone   atomic.Uint32  // Number of resize chunks completed
	slotsRaw   unsafe.Pointer // keeps the typed backing array alive for GC
}

type dwhtEntry[K comparable, V any] struct {
	key K
	val V
}

type dwhtSlotRaw struct {
	ctrl  uintptr
	entry unsafe.Pointer
}

type dwhtSlotRawRot8 struct {
	entry unsafe.Pointer
	ctrl  uintptr
}

type dwhtSlotWords [2]uintptr

const dwhtSlotBytes = unsafe.Sizeof(dwhtSlotWords{})

// var (
// 	_ [16 - dwhtSlotBytes]byte
// 	_ [dwhtSlotBytes - 16]byte
// )

const (
	dwhtSlotLayoutUnknown uint32 = iota
	dwhtSlotLayoutRaw
	dwhtSlotLayoutRot8
)

var dwhtSlotLayoutHints [bits.UintSize]atomic.Uint32

//go:nosplit
func (t *dwhtTable[K, V]) slot(i uintptr) *[2]uintptr {
	return (*[2]uintptr)(unsafe.Add(t.slotsBase, i*dwhtSlotBytes))
}

const (
	dwhtStateMask    = uint64(0x3)
	dwhtStateEmpty   = uint64(0)
	dwhtStateFull    = uint64(1)
	dwhtStateDeleted = uint64(2)

	dwhtH2Shift = 2
	dwhtH2Mask  = uint64(0xFFFFFFFF)

	dwhtSeqShift = 34
	dwhtSeqInc   = uint64(1) << dwhtSeqShift

	dwhtFrozen = uint64(1) << 63
)

type dwhtStoreStatus uint8

const (
	dwhtStoreOK dwhtStoreStatus = iota
	dwhtStoreFrozen
	dwhtStoreFull
	dwhtStoreRetry
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
		h = uint32(hash ^ (hash >> 32))
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	start := dwhtStart(h, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dwhtMaxProbeThreshold {
			return *new(V), false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		switch dwhtCtrlState(uint64(ctrl)) {
		case dwhtStateEmpty:
			return *new(V), false
		case dwhtStateFull:
			if dwhtCtrlH2(uint64(ctrl)) != h {
				continue
			}
			entryPtr := atomic.LoadUintptr(&slot[1])
			if entryPtr == 0 {
				continue
			}
			entry := (*dwhtEntry[K, V])(unsafe.Pointer(entryPtr)) //nolint:all
			if entry.key == key {
				return entry.val, true
			}
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
		h = uint32(hash ^ (hash >> 32))
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, _, _ := m.storeInto(table, noEscape(&key), noEscape(&value), h, false)
		switch status {
		case dwhtStoreOK:
			m.growIfNeeded(table)
			return
		case dwhtStoreFrozen:
			table = m.afterFrozenTable(table)
		case dwhtStoreFull:
			m.tryGrow(table)
			table = m.ensureTable()
		case dwhtStoreRetry:
			table = m.ensureTable()
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
		h = uint32(hash ^ (hash >> 32))
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, actual, loaded := m.storeInto(table, noEscape(&key), noEscape(&value), h, true)
		switch status {
		case dwhtStoreOK:
			if !loaded {
				m.growIfNeeded(table)
			}
			return actual, loaded
		case dwhtStoreFrozen:
			table = m.afterFrozenTable(table)
		case dwhtStoreFull:
			m.tryGrow(table)
			table = m.ensureTable()
		case dwhtStoreRetry:
			table = m.ensureTable()
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
		h = uint32(hash ^ (hash >> 32))
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
			table = m.afterFrozenTable(table)
		case dwhtStoreFull:
			m.tryGrow(table)
			table = m.ensureTable()
		case dwhtStoreRetry:
			table = m.ensureTable()
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
		h = uint32(hash ^ (hash >> 32))
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, _, _ := m.deleteFrom(table, noEscape(&key), h, false)
		if status == dwhtStoreOK {
			return
		}
		table = m.afterFrozenTable(table)
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
		h = uint32(hash ^ (hash >> 32))
	} else {
		hash := m.keyHash(noescape(unsafe.Pointer(&key)), m.seed)
		h = uint32(hash ^ (hash >> 32))
	}
	for {
		status, prev, loaded := m.deleteFrom(table, noEscape(&key), h, true)
		if status == dwhtStoreOK {
			return prev, loaded
		}
		table = m.afterFrozenTable(table)
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
		h = uint32(hash ^ (hash >> 32))
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
		table = m.afterFrozenTable(table)
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
		h = uint32(hash ^ (hash >> 32))
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
		table = m.afterFrozenTable(table)
	}
}

// Range iterates over a weakly consistent snapshot of the table.
func (m *DWHTMap[K, V]) Range(yield func(K, V) bool) {
	table := m.table.Load()
	if table == nil {
		return
	}
	for i := uintptr(0); i <= table.mask; i++ {
		slot := table.slot(i)
		ctrl := atomic.LoadUintptr(&slot[0])
		if dwhtCtrlState(uint64(ctrl)) != dwhtStateFull {
			continue
		}
		entryPtr := atomic.LoadUintptr(&slot[1])
		if entryPtr == 0 {
			continue
		}
		entry := (*dwhtEntry[K, V])(unsafe.Pointer(entryPtr)) //nolint:all
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
	return int(m.size.Value())
}

func (m *DWHTMap[K, V]) ensureTable() *dwhtTable[K, V] {
	table := m.table.Load()
	if table != nil {
		return table
	}
	// Lock-free init
	newTab := newDWHTTable[K, V](m.minLen)
	if m.table.CompareAndSwap(nil, newTab) {
		return newTab
	}
	return m.table.Load()
}

// //go:nosplit
// func (m *DWHTMap[K, V]) hashKey(key *K) (uintptr, uint16) {
// 	if dwhtEnableIntKey {
// 		if m.intKey {
// 			h1v := intHash[K](noescape(unsafe.Pointer(key)))
// 			return h1v, uint16(h1v ^ (h1v >> 16))
// 		}
// 	}
// 	h1v := m.keyHash(noescape(unsafe.Pointer(key)), m.seed)
// 	return h1v >> 16, uint16(h1v)
// }

func (m *DWHTMap[K, V]) storeInto(
	table *dwhtTable[K, V],
	key *K,
	val *V,
	h uint32,
	onlyIfAbsent bool,
) (dwhtStoreStatus, V, bool) {
	var (
		deleted   *[2]uintptr
		deletedC  uintptr
		deletedE  uintptr
		deletedOK bool
		newEntry  *dwhtEntry[K, V]
	)
	start := dwhtStart(h, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dwhtMaxProbeThreshold {
			return dwhtStoreFull, *new(V), false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dwhtFrozen != 0 {
			return dwhtStoreFrozen, *new(V), false
		}

		entry := atomic.LoadUintptr(&slot[1])

		switch dwhtCtrlState(uint64(ctrl)) {
		case dwhtStateFull:
			if dwhtCtrlH2(uint64(ctrl)) != h {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dwhtEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
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
			newCtrl := dwhtCtrlUpdate(uint64(ctrl))
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
				probe--
				continue
			}
			return dwhtStoreOK, *val, true
		case dwhtStateDeleted:
			if !deletedOK {
				deleted, deletedC, deletedE, deletedOK = slot, ctrl, entry, true
			}
		case dwhtStateEmpty:
			if deletedOK {
				status, rVal, loaded := m.claimSlot(deleted, deletedC, deletedE, key, val, h, newEntry)
				if status == dwhtStoreRetry {
					return dwhtStoreRetry, *new(V), false
				}
				return status, rVal, loaded
			}
			if newEntry == nil {
				newEntry = &dwhtEntry[K, V]{key: *key, val: *val}
			}
			newCtrl := dwhtCtrlInsert(uint64(ctrl), h)
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
				probe--
				continue
			}
			m.size.Add(1)
			return dwhtStoreOK, *val, false
		}
	}
	if deletedOK {
		return m.claimSlot(deleted, deletedC, deletedE, key, val, h, newEntry)
	}
	return dwhtStoreFull, *new(V), false
}

func (m *DWHTMap[K, V]) claimSlot(
	slot *[2]uintptr,
	ctrl uintptr,
	entry uintptr,
	key *K,
	val *V,
	h2v uint32,
	newEntry *dwhtEntry[K, V],
) (dwhtStoreStatus, V, bool) {
	if uint64(ctrl)&dwhtFrozen != 0 {
		return dwhtStoreFrozen, *new(V), false
	}
	if newEntry == nil {
		newEntry = &dwhtEntry[K, V]{key: *key, val: *val}
	}
	newCtrl := dwhtCtrlInsert(uint64(ctrl), h2v)
	if !asm.DWCAS(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
		return dwhtStoreRetry, *new(V), false
	}
	m.size.Add(1)
	return dwhtStoreOK, *val, false
}

func (m *DWHTMap[K, V]) loadAndUpdateIn(
	table *dwhtTable[K, V],
	key *K,
	val *V,
	h uint32,
) (dwhtStoreStatus, V, bool) {
	var newEntry *dwhtEntry[K, V]
	start := dwhtStart(h, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dwhtMaxProbeThreshold {
			return dwhtStoreFull, *new(V), false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dwhtFrozen != 0 {
			return dwhtStoreFrozen, *new(V), false
		}
		entry := atomic.LoadUintptr(&slot[1])

		switch dwhtCtrlState(uint64(ctrl)) {
		case dwhtStateEmpty:
			return dwhtStoreOK, *new(V), false
		case dwhtStateFull:
			if dwhtCtrlH2(uint64(ctrl)) != h {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dwhtEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
			if e.key != *key {
				continue
			}
			if newEntry == nil {
				newEntry = &dwhtEntry[K, V]{key: *key, val: *val}
			}
			newCtrl := dwhtCtrlUpdate(uint64(ctrl))
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
				probe--
				continue
			}
			return dwhtStoreOK, e.val, true
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
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dwhtMaxProbeThreshold {
			return dwhtStoreFull, *new(V), false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dwhtFrozen != 0 {
			return dwhtStoreFrozen, *new(V), false
		}

		entry := atomic.LoadUintptr(&slot[1])

		switch dwhtCtrlState(uint64(ctrl)) {
		case dwhtStateEmpty:
			return dwhtStoreOK, *new(V), false
		case dwhtStateFull:
			if dwhtCtrlH2(uint64(ctrl)) != h {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dwhtEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
			if e.key != *key {
				continue
			}

			newCtrl := dwhtCtrlDelete(uint64(ctrl))
			// We can nil the entry to help GC, or keep it to allow lock-free readers to finish
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(nil)) { //nolint:all
				probe--
				continue
			}

			var prev V
			if needValue {
				prev = e.val
			}

			m.size.Add(^uintptr(0))
			return dwhtStoreOK, prev, true
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
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dwhtMaxProbeThreshold {
			return dwhtStoreFull, false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dwhtFrozen != 0 {
			return dwhtStoreFrozen, false
		}
		entry := atomic.LoadUintptr(&slot[1])

		switch dwhtCtrlState(uint64(ctrl)) {
		case dwhtStateEmpty:
			return dwhtStoreOK, false
		case dwhtStateFull:
			if dwhtCtrlH2(uint64(ctrl)) != h {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dwhtEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
			if e.key != *key {
				continue
			}
			if m.valEqual == nil {
				panic("cc: value is not comparable; use WithValueEqual")
			}
			if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
				return dwhtStoreOK, false
			}

			if newEntry == nil {
				newEntry = &dwhtEntry[K, V]{key: *key, val: *newVal}
			}
			newCtrl := dwhtCtrlUpdate(uint64(ctrl))
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
				probe--
				continue
			}
			return dwhtStoreOK, true
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
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dwhtMaxProbeThreshold {
			return dwhtStoreFull, false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dwhtFrozen != 0 {
			return dwhtStoreFrozen, false
		}
		entry := atomic.LoadUintptr(&slot[1])

		switch dwhtCtrlState(uint64(ctrl)) {
		case dwhtStateEmpty:
			return dwhtStoreOK, false
		case dwhtStateFull:
			if dwhtCtrlH2(uint64(ctrl)) != h {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dwhtEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
			if e.key != *key {
				continue
			}
			if m.valEqual == nil {
				panic("cc: value is not comparable; use WithValueEqual")
			}
			if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
				return dwhtStoreOK, false
			}

			newCtrl := dwhtCtrlDelete(uint64(ctrl))
			if !asm.DWCAS(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(nil)) { //nolint:all
				probe--
				continue
			}

			m.size.Add(^uintptr(0))
			return dwhtStoreOK, true
		}
	}
	return dwhtStoreOK, false
}

func (m *DWHTMap[K, V]) growIfNeeded(table *dwhtTable[K, V]) {
	localSize := int(m.size.Get().Load())
	if localSize&dwhtGrowCheckMask == 0 {
		if localSize >= table.stripeCap {
			if int(m.size.Value()) >= table.growCap {
				m.tryGrow(table)
			}
		}
	}
}

func (m *DWHTMap[K, V]) tryGrow(old *dwhtTable[K, V]) {
	if m.table.Load() != old {
		return
	}

	next := old.nextTable.Load()
	if next == nil {
		if old.allocating.CompareAndSwap(0, 1) {
			newTable := newDWHTTable[K, V](m.nextGrowSlotLen(old))
			old.nextTable.CompareAndSwap(nil, newTable)
			next = old.nextTable.Load()
		} else {
			// Wait for leader to allocate
			// for {
			// 	next = old.nextTable.Load()
			// 	if next != nil {
			// 		break
			// 	}
			// 	runtime.Gosched()
			// }
			return // Fallback to retry in caller
		}
	}

	slotLen := old.mask + 1
	cpus := maxProcs()
	chunks := uint32(calcParallelism(slotLen, cpus*resizeOverPartition))
	chunkSz := uintptr(max(1, slotLen>>bits.TrailingZeros32(chunks)))

	// Cooperative resize.
	// Each slot is frozen before it is copied. This removes the old global
	// freeze barrier while preserving the key rule: a copied slot can no longer
	// be modified in the old table.
	for {
		chunk := old.copyIdx.Add(1) - 1
		if chunk >= chunks {
			break
		}
		start := uintptr(chunk) * chunkSz
		end := min(start+chunkSz, slotLen)
		for i := start; i < end; i++ {
			slot := old.slot(i)
			var ctrl uintptr
			var entry uintptr
			for {
				ctrl = atomic.LoadUintptr(&slot[0])
				if uint64(ctrl)&dwhtFrozen != 0 {
					entry = atomic.LoadUintptr(&slot[1])
					break
				}
				entry = atomic.LoadUintptr(&slot[1])
				if !asm.DWCAS(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(uint64(ctrl)|dwhtFrozen), unsafe.Pointer(entry)) { //nolint:all
					continue
				}
				ctrl = uintptr(uint64(ctrl) | dwhtFrozen)
				break
			}

			if dwhtCtrlState(uint64(ctrl)) != dwhtStateFull {
				continue
			}
			entryPtr := entry
			if entryPtr == 0 {
				continue
			}

			h := dwhtCtrlH2(uint64(ctrl))
			destStart := dwhtStart(h, next.mask)
			// copied := false
			for probe := uintptr(0); probe <= next.mask; probe++ {
				destSlot := next.slot((destStart + probe) & next.mask)
				destCtrl := atomic.LoadUintptr(&destSlot[0])
				if dwhtCtrlState(uint64(destCtrl)) != dwhtStateEmpty {
					continue
				}
				destEntry := atomic.LoadUintptr(&destSlot[1])
				newCtrl := dwhtCtrlInsert(uint64(destCtrl), h)
				if !asm.DWCAS(unsafe.Pointer(destSlot), destCtrl, unsafe.Pointer(destEntry), uintptr(newCtrl), unsafe.Pointer(entryPtr)) { //nolint:all
					probe--
					continue
				}
				// copied = true
				break
			}
			// if !copied {
			// 	panic("cc: DWHTMap grow produced a full table")
			// }
		}
		old.copyDone.Add(1)
	}

	for old.copyDone.Load() < chunks {
		runtime.Gosched()
	}

	m.table.CompareAndSwap(old, next)
}

func (m *DWHTMap[K, V]) nextGrowSlotLen(old *dwhtTable[K, V]) uintptr {
	slotLen := old.mask + 1
	nextLen := slotLen << 1
	if !dwhtEnableAggressiveGrow {
		return nextLen
	}

	// Resize does not stop all writers immediately; goroutines that already
	// hold old can keep inserting into slots that have not been frozen yet.
	// When that concurrent window makes old much denser than the normal grow
	// threshold, grow for roughly another table worth of inserts instead of
	// forcing a near-immediate second resize.
	size := m.size.Value()
	if size > 0 {
		nextLen = max(nextLen, dwhtCalcSlotLen(size<<1))
	}
	return nextLen
}

func (m *DWHTMap[K, V]) afterFrozenTable(old *dwhtTable[K, V]) *dwhtTable[K, V] {
	for {
		table := m.table.Load()
		if table != old {
			return table
		}
		m.tryGrow(old)
	}
}

func newDWHTTable[K comparable, V any](slotLen uintptr) *dwhtTable[K, V] {
	slotLen = nextPowOf2(max(slotLen, dwhtMinSlots))
	base, raw := makeDWHTSlots(slotLen)
	growCap := int(float64(slotLen) * dwhtLoadFactor)
	roundedSizeLen := nextPowOf2(maxProcs())
	return &dwhtTable[K, V]{
		slotsBase: base,
		slotsRaw:  raw,
		mask:      slotLen - 1,
		stripeCap: int(growCap >> bits.TrailingZeros32(uint32(roundedSizeLen))),
		growCap:   growCap,
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

	panic("cc: DWHTMap slot storage is not 16-byte aligned")
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

func dwhtCalcSlotLen(capacity uintptr) uintptr {
	if capacity == 0 {
		return dwhtMinSlots
	}
	const invLoadFactor = 1 / dwhtLoadFactor
	need := uintptr(float64(capacity+1) * invLoadFactor)
	return nextPowOf2(max(need, dwhtMinSlots))
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
