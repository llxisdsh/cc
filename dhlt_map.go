package cc

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	dhltEnableIntKey   = false
	dhltEnableDedupVal = false
)

// DHLTMap is an experimental DHLT-style hash map (Double-Word CAS Lock-free Table).
//
// The implementation uses open addressing, cache-line friendly slot groups, and per-slot
// DWCAS state transitions. By using DWCAS, it can atomically publish both a control word
// and an *entry pointer, completely eliminating the "busy" state found in OFHTMap.
//
// Concurrency model:
//   - Loads are wait-free and read the entry pointer directly.
//   - Writes reserve one slot with DWCAS, replacing the empty state with a full state
//     and the new entry pointer in one atomic hardware instruction.
//   - Grow freezes the old table slot-by-slot before copying. Writers that hit
//     a frozen slot help resize and retry on the newly published table without a global lock.
type DHLTMap[K comparable, V any] struct {
	_        noCopy
	table    atomic.Pointer[dhltTable[K, V]]
	intKey   bool
	seed     uintptr
	keyHash  HashFunc
	valEqual EqualFunc
	minLen   uintptr
	size     PLocalCounter
}

type dhltTable[K comparable, V any] struct {
	slotsBase  unsafe.Pointer
	mask       uintptr
	stripeCap  int
	growCap    uintptr
	nextTable  atomic.Pointer[dhltTable[K, V]]
	allocating atomic.Uint32    // 0: no one is allocating, 1: allocating
	freezeIdx  atomic.Uint32    // Next chunk index for cooperative resizing (Phase 1: freeze)
	frozenDone atomic.Uint32    // Number of freeze chunks completed
	copyIdx    atomic.Uint32    // Next chunk index for cooperative resizing (Phase 2: copy)
	copyDone   atomic.Uint32    // Number of copy chunks completed
	slotsRaw   []unsafe.Pointer // kept to prevent GC collection of the underlying array
}

type dhltEntry[K comparable, V any] struct {
	key K
	val V
}

type dhltSlotWords [2]uintptr

const dhltSlotBytes = unsafe.Sizeof(dhltSlotWords{})

var (
	_ [16 - dhltSlotBytes]byte
	_ [dhltSlotBytes - 16]byte
)

//go:nosplit
func (t *dhltTable[K, V]) slot(i uintptr) *[2]uintptr {
	return (*[2]uintptr)(unsafe.Add(t.slotsBase, i*dhltSlotBytes))
}

const (
	dhltGroupSlots = uintptr(8)
	dhltMinSlots   = uintptr(64)

	// dhltNotPtr is always set on non-empty ctrl words to prevent GC from
	// treating the ctrl word as a valid pointer, saving scan time.
	dhltNotPtr = uint64(1)

	dhltStateMask    = uint64(0x3) << 1
	dhltStateEmpty   = uint64(0) << 1
	dhltStateFull    = uint64(1) << 1
	dhltStateDeleted = uint64(2) << 1

	dhltH2Shift = 3
	dhltH2Mask  = uint64(0xFFFF)

	dhltSeqShift = 19
	dhltSeqInc   = uint64(1) << dhltSeqShift

	dhltFrozen = uint64(1) << 63

	dhltGrowNumerator   = uintptr(3)
	dhltGrowDenominator = uintptr(4)

	// dhltMaxProbeThreshold is the threshold of linear probing depth.
	// If a store operation probes more than this many slots without success,
	// it will eagerly trigger a resize even if the table is not fully loaded.
	dhltMaxProbeThreshold = uintptr(128)

	// dhltGrowCheckMask is used as a bitwise AND mask to sample the local size counter.
	// This reduces the overhead of checking the global size on every insertion.
	// It MUST be strictly smaller than the initial grow threshold.
	// Since dhltMinSlots is 64 and the grow factor is 3/4, the first grow
	// happens at 48. If this mask is too large (e.g., 63), the table could fill up
	// completely without triggering a resize in highly concurrent cold starts.
	dhltGrowCheckMask = 7 // Checks every 8th local insert
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
		slot := table.slot(i)
		ctrl := atomic.LoadUintptr(&slot[0])
		if dhltCtrlState(uint64(ctrl)) != dhltStateFull {
			continue
		}
		entryPtr := atomic.LoadUintptr(&slot[1])
		if entryPtr == 0 {
			continue
		}
		entry := (*dhltEntry[K, V])(unsafe.Pointer(entryPtr)) //nolint:all
		if !yield(entry.key, entry.val) {
			return
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
	// Lock-free init
	newTab := newDHLTTable[K, V](m.minLen)
	if m.table.CompareAndSwap(nil, newTab) {
		return newTab
	}
	return m.table.Load()
}

//go:nosplit
func (m *DHLTMap[K, V]) hashKey(key *K) (uintptr, uint16) {
	if dhltEnableIntKey {
		if m.intKey {
			h1v := intHash[K](noescape(unsafe.Pointer(key)))
			return h1v, uint16(h1v ^ (h1v >> 16))
		}
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
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dhltMaxProbeThreshold {
			return *new(V), false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		switch dhltCtrlState(uint64(ctrl)) {
		case dhltStateEmpty:
			return *new(V), false
		case dhltStateFull:
			if dhltCtrlH2(uint64(ctrl)) != h2v {
				continue
			}
			entryPtr := atomic.LoadUintptr(&slot[1])
			if entryPtr == 0 {
				continue
			}
			entry := (*dhltEntry[K, V])(unsafe.Pointer(entryPtr)) //nolint:all
			if entry.key == *key {
				return entry.val, true
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
		deleted   *[2]uintptr
		deletedC  uintptr
		deletedE  uintptr
		deletedOK bool
		newEntry  *dhltEntry[K, V]
	)
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dhltMaxProbeThreshold {
			return dhltStoreFull, *new(V), false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dhltFrozen != 0 {
			return dhltStoreFrozen, *new(V), false
		}

		entry := atomic.LoadUintptr(&slot[1])

		switch dhltCtrlState(uint64(ctrl)) {
		case dhltStateFull:
			if dhltCtrlH2(uint64(ctrl)) != h2v {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dhltEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
			if e.key != *key {
				continue
			}
			if onlyIfAbsent {
				return dhltStoreOK, e.val, true
			}
			if dhltEnableDedupVal {
				if m.valEqual != nil && m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(val))) {
					return dhltStoreOK, e.val, true
				}
			}

			if newEntry == nil {
				newEntry = &dhltEntry[K, V]{key: *key, val: *val}
			}
			newCtrl := dhltCtrlUpdate(uint64(ctrl))
			if !dwcas(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
				probe--
				continue
			}
			return dhltStoreOK, *val, true
		case dhltStateDeleted:
			if !deletedOK {
				deleted, deletedC, deletedE, deletedOK = slot, ctrl, entry, true
			}
		case dhltStateEmpty:
			if deletedOK {
				status, rVal, loaded := m.claimSlot(deleted, deletedC, deletedE, key, val, h2v, newEntry)
				if status == dhltStoreRetry {
					return dhltStoreRetry, *new(V), false
				}
				return status, rVal, loaded
			}
			if newEntry == nil {
				newEntry = &dhltEntry[K, V]{key: *key, val: *val}
			}
			newCtrl := dhltCtrlInsert(uint64(ctrl), h2v)
			if !dwcas(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
				probe--
				continue
			}
			m.size.Add(1)
			return dhltStoreOK, *val, false
		}
	}
	if deletedOK {
		return m.claimSlot(deleted, deletedC, deletedE, key, val, h2v, newEntry)
	}
	return dhltStoreFull, *new(V), false
}

func (m *DHLTMap[K, V]) claimSlot(
	slot *[2]uintptr,
	ctrl uintptr,
	entry uintptr,
	key *K,
	val *V,
	h2v uint16,
	newEntry *dhltEntry[K, V],
) (dhltStoreStatus, V, bool) {
	if uint64(ctrl)&dhltFrozen != 0 {
		return dhltStoreFrozen, *new(V), false
	}
	if newEntry == nil {
		newEntry = &dhltEntry[K, V]{key: *key, val: *val}
	}
	newCtrl := dhltCtrlInsert(uint64(ctrl), h2v)
	if !dwcas(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
		return dhltStoreRetry, *new(V), false
	}
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
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dhltMaxProbeThreshold {
			return dhltStoreFull, *new(V), false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dhltFrozen != 0 {
			return dhltStoreFrozen, *new(V), false
		}

		entry := atomic.LoadUintptr(&slot[1])

		switch dhltCtrlState(uint64(ctrl)) {
		case dhltStateEmpty:
			return dhltStoreOK, *new(V), false
		case dhltStateFull:
			if dhltCtrlH2(uint64(ctrl)) != h2v {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dhltEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
			if e.key != *key {
				continue
			}

			newCtrl := dhltCtrlDelete(uint64(ctrl))
			// We can nil the entry to help GC, or keep it to allow lock-free readers to finish
			if !dwcas(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(nil)) { //nolint:all
				probe--
				continue
			}

			var prev V
			if needValue {
				prev = e.val
			}

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
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dhltMaxProbeThreshold {
			return dhltStoreFull, false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dhltFrozen != 0 {
			return dhltStoreFrozen, false
		}
		entry := atomic.LoadUintptr(&slot[1])

		switch dhltCtrlState(uint64(ctrl)) {
		case dhltStateEmpty:
			return dhltStoreOK, false
		case dhltStateFull:
			if dhltCtrlH2(uint64(ctrl)) != h2v {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dhltEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
			if e.key != *key {
				continue
			}
			if m.valEqual == nil {
				panic("cc: value is not comparable; use WithValueEqual")
			}
			if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
				return dhltStoreOK, false
			}

			newEntry := &dhltEntry[K, V]{key: *key, val: *newVal}
			newCtrl := dhltCtrlUpdate(uint64(ctrl))
			if !dwcas(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(newEntry)) { //nolint:all
				probe--
				continue
			}
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
	start := dhltStart(h1v, table.mask)
	for probe := uintptr(0); probe <= table.mask; probe++ {
		if probe > dhltMaxProbeThreshold {
			return dhltStoreFull, false
		}
		slot := table.slot((start + probe) & table.mask)
		ctrl := atomic.LoadUintptr(&slot[0])
		if uint64(ctrl)&dhltFrozen != 0 {
			return dhltStoreFrozen, false
		}
		entry := atomic.LoadUintptr(&slot[1])

		switch dhltCtrlState(uint64(ctrl)) {
		case dhltStateEmpty:
			return dhltStoreOK, false
		case dhltStateFull:
			if dhltCtrlH2(uint64(ctrl)) != h2v {
				continue
			}
			if entry == 0 {
				continue
			}
			e := (*dhltEntry[K, V])(unsafe.Pointer(entry)) //nolint:all
			if e.key != *key {
				continue
			}
			if m.valEqual == nil {
				panic("cc: value is not comparable; use WithValueEqual")
			}
			if !m.valEqual(noescape(unsafe.Pointer(&e.val)), noescape(unsafe.Pointer(old))) {
				return dhltStoreOK, false
			}

			newCtrl := dhltCtrlDelete(uint64(ctrl))
			if !dwcas(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(newCtrl), unsafe.Pointer(nil)) { //nolint:all
				probe--
				continue
			}

			m.size.Add(^uintptr(0))
			return dhltStoreOK, true
		}
	}
	return dhltStoreOK, false
}

func (m *DHLTMap[K, V]) growIfNeeded(table *dhltTable[K, V]) {
	localSize := int(m.size.Get().Load())
	if localSize&dhltGrowCheckMask == 0 {
		if localSize >= table.stripeCap {
			if m.size.Value() >= table.growCap {
				m.tryGrow(table, (table.mask+1)<<1)
			}
		}
	}
}

func (m *DHLTMap[K, V]) tryGrow(old *dhltTable[K, V], newLen uintptr) {
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

			// NOTE: We MUST ensure the new table has enough capacity!
			neededSize := int(m.size.Value()) * 2 // Roughly double the current size
			neededLen := dhltCalcSlotLen(uintptr(neededSize))
			if neededLen > newLen {
				newLen = neededLen
			}

			newTable := newDHLTTable[K, V](newLen)
			old.nextTable.CompareAndSwap(nil, newTable)
			next = old.nextTable.Load()
		} else {
			// Wait for leader to allocate
			for range 16 {
				next = old.nextTable.Load()
				if next != nil {
					break
				}
			}
			if next == nil {
				return // Fallback to retry in caller
			}
		}
	}

	// Phase 1: Freeze all slots concurrently
	slotLen := old.mask + 1
	cpus := maxProcs()
	chunks := uint32(calcParallelism(slotLen, cpus*resizeOverPartition))
	chunkSz := uintptr(max(1, slotLen>>bits.TrailingZeros32(chunks)))

	for {
		chunk := old.freezeIdx.Add(1) - 1
		if chunk >= chunks {
			break
		}
		start := uintptr(chunk) * chunkSz
		end := min(start+chunkSz, slotLen)
		for i := start; i < end; i++ {
			slot := old.slot(i)
			for {
				ctrl := atomic.LoadUintptr(&slot[0])
				if uint64(ctrl)&dhltFrozen != 0 {
					break
				}
				entry := atomic.LoadUintptr(&slot[1])
				if !dwcas(unsafe.Pointer(slot), ctrl, unsafe.Pointer(entry), uintptr(uint64(ctrl)|dhltFrozen), unsafe.Pointer(entry)) { //nolint:all
					break
				}
			}
		}
		old.frozenDone.Add(1)
	}

	// Wait for all threads to finish freezing.
	// This acts as a barrier to ensure no new writes can occur before copying starts.
	for old.frozenDone.Load() < chunks {
		runtime.Gosched()
	}

	// Phase 2: Copy chunks
	for {
		chunk := old.copyIdx.Add(1) - 1
		if chunk >= chunks {
			break
		}
		start := uintptr(chunk) * chunkSz
		end := min(start+chunkSz, slotLen)
		for i := start; i < end; i++ {
			slot := old.slot(i)
			ctrl := atomic.LoadUintptr(&slot[0])
			if dhltCtrlState(uint64(ctrl)) != dhltStateFull {
				continue
			}
			entryPtr := atomic.LoadUintptr(&slot[1])
			if entryPtr == 0 {
				continue
			}
			e := (*dhltEntry[K, V])(unsafe.Pointer(entryPtr)) //nolint:all
			h1v, h2v := m.hashKey(&e.key)

			destStart := dhltStart(h1v, next.mask)
			for probe := uintptr(0); probe <= next.mask; probe++ {
				destSlot := next.slot((destStart + probe) & next.mask)
				destCtrl := atomic.LoadUintptr(&destSlot[0])
				if dhltCtrlState(uint64(destCtrl)) != dhltStateEmpty {
					continue
				}
				destEntry := atomic.LoadUintptr(&destSlot[1])
				newCtrl := dhltCtrlInsert(uint64(destCtrl), h2v)
				if !dwcas(unsafe.Pointer(destSlot), destCtrl, unsafe.Pointer(destEntry), uintptr(newCtrl), unsafe.Pointer(entryPtr)) { //nolint:all
					probe--
					continue
				}
				break
			}
		}
		old.copyDone.Add(1)
	}

	for old.copyDone.Load() < chunks {
		runtime.Gosched()
	}

	m.table.CompareAndSwap(old, next)
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

	raw := make([]unsafe.Pointer, int(slotLen)*len(dhltSlotWords{})+1)
	base := unsafe.Pointer(&raw[0])
	if uintptr(base)%16 != 0 {
		base = unsafe.Pointer(&raw[1])
	}

	growCap := (slotLen * dhltGrowNumerator) / dhltGrowDenominator
	roundedSizeLen := nextPowOf2(maxProcs())
	return &dhltTable[K, V]{
		slotsBase: base,
		slotsRaw:  raw,
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
	return uint16(ctrl >> dhltH2Shift)
}

//go:nosplit
func dhltCtrlInsert(ctrl uint64, h2v uint16) uint64 {
	c := (ctrl &^ dhltStateMask) | dhltStateFull | dhltNotPtr
	c = (c &^ (dhltH2Mask << dhltH2Shift)) | (uint64(h2v) << dhltH2Shift)
	return c + dhltSeqInc
}

//go:nosplit
func dhltCtrlUpdate(ctrl uint64) uint64 {
	return (ctrl+dhltSeqInc)&^dhltStateMask | dhltStateFull | dhltNotPtr
}

//go:nosplit
func dhltCtrlDelete(ctrl uint64) uint64 {
	return (ctrl+dhltSeqInc)&^dhltStateMask | dhltStateDeleted | dhltNotPtr
}
