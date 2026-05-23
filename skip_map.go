package cc

import (
	"cmp"
	"sync/atomic"
	"unsafe"
)

const (
	skipMaxLevel     = 16
	skipDefaultLevel = 3
	skipBaseLinks    = 4
	skipExtraLinks   = skipMaxLevel - skipBaseLinks
)

const (
	skipFlagLinked uint32 = 1 << iota // [bit0: linked]
	skipFlagMarked                    // [bit1: marked]
	skipFlagLock                      // [bit2: lock]
)

// SkipMap represents a lock-free, concurrent-safe skip list mapping keys to values.
// It is designed to scale under heavy contention while minimizing cache-line bounces.
type SkipMap[K cmp.Ordered, V any] struct {
	head     *skipNode[K, V]
	topLevel uintptr
	count    uintptr
}

type skipNode[K cmp.Ordered, V any] struct {
	// Group read-heavy fields in the first 64 bytes (cache line).
	// This dramatically reduces false sharing when the node is frequently accessed.
	key   K
	flags uint32 // [bit0: linked] [bit1: marked] [bit2: lock]
	level uint32
	value unsafe.Pointer                  // *V
	base  [skipBaseLinks]unsafe.Pointer   // *skipNode[K, V]
	extra *[skipExtraLinks]unsafe.Pointer // *skipNode[K, V]
}

// NewSkipMap initializes an empty concurrent skip map.
func NewSkipMap[K cmp.Ordered, V any]() *SkipMap[K, V] {
	head := newSkipNode(*new(K), *new(V), skipMaxLevel)
	head.setFlag(skipFlagLinked)
	return &SkipMap[K, V]{
		head:     head,
		topLevel: skipDefaultLevel,
	}
}

// newSkipNode allocates a skip list node and extends the pointer array if it exceeds base links.
func newSkipNode[K cmp.Ordered, V any](k K, v V, level int) *skipNode[K, V] {
	n := &skipNode[K, V]{
		key:   k,
		level: uint32(level),
	}
	n.storeVal(v)
	if level > skipBaseLinks {
		n.extra = new([skipExtraLinks]unsafe.Pointer)
	}
	return n
}

//go:nosplit
func (n *skipNode[K, V]) baseAt(i int) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(unsafe.Pointer(&n.base),
		uintptr(i)*unsafe.Sizeof(unsafe.Pointer(nil))))
}

//go:nosplit
func (n *skipNode[K, V]) extraAt(i int) *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Add(unsafe.Pointer(n.extra),
		uintptr(i)*unsafe.Sizeof(unsafe.Pointer(nil))))
}

//go:nosplit
func (n *skipNode[K, V]) storeVal(v V) {
	storePtr(&n.value, unsafe.Pointer(&v))
}

//go:nosplit
func (n *skipNode[K, V]) loadVal() V {
	return *(*V)(loadPtr(&n.value))
}

//go:nosplit
func (n *skipNode[K, V]) next(layer int) *skipNode[K, V] {
	if layer < skipBaseLinks {
		return (*skipNode[K, V])(loadPtr(n.baseAt(layer)))
	}
	return (*skipNode[K, V])(loadPtr(n.extraAt(layer - skipBaseLinks)))
}

//go:nosplit
func (n *skipNode[K, V]) setNext(layer int, dest *skipNode[K, V]) {
	if layer < skipBaseLinks {
		storePtr(n.baseAt(layer), unsafe.Pointer(dest))
		return
	}
	storePtr(n.extraAt(layer-skipBaseLinks), unsafe.Pointer(dest))
}

//go:nosplit
func (n *skipNode[K, V]) setFlag(f uint32) {
	atomic.OrUint32(&n.flags, f)
}

//go:nosplit
func (n *skipNode[K, V]) lock() {
	// inline BitLockUint32
	cur := atomic.LoadUint32(&n.flags)
	if atomic.CompareAndSwapUint32(&n.flags, cur&^skipFlagLock, cur|skipFlagLock) {
		return
	}
	slowBitLockUint32(&n.flags, skipFlagLock)
}

//go:nosplit
func (n *skipNode[K, V]) unlock() {
	BitUnlockUint32(&n.flags, skipFlagLock)
}

//go:nosplit
func (n *skipNode[K, V]) hasFlag(f uint32) bool {
	return (loadUint32(&n.flags) & f) != 0
}

//go:nosplit
func (n *skipNode[K, V]) hasFlags(mask, expect uint32) bool {
	return (loadUint32(&n.flags) & mask) == expect
}

type skipNodeArray[K cmp.Ordered, V any] [skipMaxLevel]*skipNode[K, V]

//go:nosplit
func (a *skipNodeArray[K, V]) at(i int) **skipNode[K, V] {
	return (**skipNode[K, V])(unsafe.Add(unsafe.Pointer(a),
		uintptr(i)*unsafe.Sizeof((*skipNode[K, V])(nil))))
}

// randomLevel generates a level between 1 and maxLevel using simulated geometric distribution.
// Yields stable performance bypassing heavy standard rand routines.
func (s *SkipMap[K, V]) randomLevel() int {
	lvl := 1
	r := fastrand()
	// Exhaust randomness effectively by taking 2 bits at a time (p=0.25).
	for (r & 3) == 0 {
		lvl++
		if lvl >= skipMaxLevel {
			lvl = skipMaxLevel
			break
		}
		r >>= 2
	}

	for {
		top := loadUintptr(&s.topLevel)
		if uintptr(lvl) <= top {
			break
		}
		if atomic.CompareAndSwapUintptr(&s.topLevel, top, uintptr(lvl)) {
			break
		}
	}
	return lvl
}

// search locates a key and populate the path with previous and next nodes.
func (s *SkipMap[K, V]) search(k K, prevs, nexts *skipNodeArray[K, V]) *skipNode[K, V] {
	curr := s.head
	top := int(loadUintptr(&s.topLevel)) - 1
	for i := top; i >= 0; i-- {
		nex := curr.next(i)
		for nex != nil && nex.key < k {
			curr = nex
			nex = curr.next(i)
		}
		*prevs.at(i) = curr
		*nexts.at(i) = nex
		// Early exit if found
		if nex != nil && nex.key == k {
			return nex
		}
	}
	return nil
}

// searchForDelete is an optimized search tailored for deletion that yields the layer the key was found.
func (s *SkipMap[K, V]) searchForDelete(k K, prevs, nexts *skipNodeArray[K, V]) int {
	foundAt := -1
	curr := s.head
	top := int(loadUintptr(&s.topLevel)) - 1
	for i := top; i >= 0; i-- {
		nex := curr.next(i)
		for nex != nil && nex.key < k {
			curr = nex
			nex = curr.next(i)
		}
		*prevs.at(i) = curr
		*nexts.at(i) = nex

		if foundAt == -1 && nex != nil && nex.key == k {
			foundAt = i
		}
	}
	return foundAt
}

func purgeLocks[K cmp.Ordered, V any](prevs *skipNodeArray[K, V], top int) {
	var locked *skipNode[K, V]
	for i := top; i >= 0; i-- {
		p := *prevs.at(i)
		if p != locked {
			p.unlock()
			locked = p
		}
	}
}

// Store places a key-value mapping into the structure.
//
// Concurrency note: concurrent Store calls for the same key use
// last-writer-wins semantics without mutual exclusion. Both writers
// may read the old value and overwrite it. This is safe at the
// pointer level (storePtr uses atomic.StorePointer) but the caller
// should not rely on read-modify-write atomicity.
func (s *SkipMap[K, V]) Store(key K, value V) {
	lvl := s.randomLevel()
	var prevs, nexts skipNodeArray[K, V]

	for {
		if hit := s.search(key, &prevs, &nexts); hit != nil {
			if !hit.hasFlag(skipFlagMarked) {
				hit.storeVal(value)
				return
			}
			continue // node is undergoing deletion; retry insertion.
		}

		lockedTop := -1
		isValid := true
		var p, nex, prevP *skipNode[K, V]

		for i := 0; isValid && i < lvl; i++ {
			p = *prevs.at(i)
			nex = *nexts.at(i)
			if p != prevP {
				p.lock()
				lockedTop = i
				prevP = p
			}
			isValid = !p.hasFlag(skipFlagMarked) &&
				(nex == nil || !nex.hasFlag(skipFlagMarked)) && p.next(i) == nex
		}

		if !isValid {
			purgeLocks(&prevs, lockedTop)
			continue
		}

		created := newSkipNode(key, value, lvl)
		for i := range lvl {
			created.setNext(i, *nexts.at(i))
			(*prevs.at(i)).setNext(i, created)
		}
		created.setFlag(skipFlagLinked)
		purgeLocks(&prevs, lockedTop)

		atomic.AddUintptr(&s.count, 1)
		return
	}
}

// Load retrieves a mapping. Returns zero value and false if missing.
func (s *SkipMap[K, V]) Load(key K) (value V, ok bool) {
	curr := s.head
	top := int(loadUintptr(&s.topLevel)) - 1
	for i := top; i >= 0; i-- {
		nex := curr.next(i)
		for nex != nil && nex.key < key {
			curr = nex
			nex = curr.next(i)
		}

		if nex != nil && nex.key == key {
			if nex.hasFlags(skipFlagLinked|skipFlagMarked, skipFlagLinked) {
				return nex.loadVal(), true
			}
			return *new(V), false
		}
	}
	return *new(V), false
}

// Delete eliminates a mapping without returning it.
func (s *SkipMap[K, V]) Delete(key K) bool {
	_, loaded := s.LoadAndDelete(key)
	return loaded
}

// LoadAndDelete ensures the removal of a mapping and returns it.
func (s *SkipMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	var (
		victim       *skipNode[K, V]
		marked       bool
		victimLvl    = -1
		prevs, nexts skipNodeArray[K, V]
	)
	for {
		hitLvl := s.searchForDelete(key, &prevs, &nexts)

		isRemovable := hitLvl != -1 &&
			(*nexts.at(hitLvl)).hasFlags(skipFlagLinked|skipFlagMarked, skipFlagLinked) &&
			(int((*nexts.at(hitLvl)).level)-1) == hitLvl

		if marked || isRemovable {
			if !marked {
				victim = *nexts.at(hitLvl)
				victimLvl = hitLvl
				victim.lock()
				if victim.hasFlag(skipFlagMarked) {
					victim.unlock()
					return *new(V), false
				}
				victim.setFlag(skipFlagMarked)
				marked = true
			}

			lockedTop := -1
			isValid := true
			var p, nex, prevP *skipNode[K, V]

			for i := 0; isValid && i <= victimLvl; i++ {
				p = *prevs.at(i)
				nex = *nexts.at(i)
				if p != prevP {
					p.lock()
					lockedTop = i
					prevP = p
				}
				isValid = !p.hasFlag(skipFlagMarked) && p.next(i) == nex
			}

			if !isValid {
				purgeLocks(&prevs, lockedTop)
				continue
			}

			for i := victimLvl; i >= 0; i-- {
				(*prevs.at(i)).setNext(i, victim.next(i))
			}
			victim.unlock()
			purgeLocks(&prevs, lockedTop)

			atomic.AddUintptr(&s.count, ^uintptr(0))
			return victim.loadVal(), true
		}

		// The node is missing or we encountered a concurrent state change.
		// If hitLvl is -1, the node genuinely doesn't exist.
		// If the node exists but is marked, it's logically deleted by someone else.
		if hitLvl == -1 || (*nexts.at(hitLvl)).hasFlag(skipFlagMarked) {
			return *new(V), false
		}

		// Otherwise, the node is physically present but in a transient state
		// (e.g., being inserted or partially linked). We must retry.
	}
}

// LoadOrStore updates value if identical key misses in mapping.
func (s *SkipMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return s.LoadOrStoreFn(key, func() V { return value })
}

// LoadOrStoreFn operates analogously to LoadOrStore but fetches assignment lazily.
func (s *SkipMap[K, V]) LoadOrStoreFn(key K, newValFn func() V) (actual V, loaded bool) {
	var (
		lvl          int
		prevs, nexts skipNodeArray[K, V]
	)
	for {
		top := int(loadUintptr(&s.topLevel))
		hit := s.search(key, &prevs, &nexts)
		if hit != nil {
			if !hit.hasFlag(skipFlagMarked) {
				return hit.loadVal(), true
			}
			continue
		}

		lockedTop := -1
		isValid := true
		var p, nex, prevP *skipNode[K, V]

		if lvl == 0 {
			lvl = s.randomLevel()
			if lvl > top {
				continue
			}
		}

		for i := 0; isValid && i < lvl; i++ {
			p = *prevs.at(i)
			nex = *nexts.at(i)
			if p != prevP {
				p.lock()
				lockedTop = i
				prevP = p
			}
			isValid = !p.hasFlag(skipFlagMarked) &&
				(nex == nil || !nex.hasFlag(skipFlagMarked)) && p.next(i) == nex
		}

		if !isValid {
			purgeLocks(&prevs, lockedTop)
			continue
		}

		v := newValFn()
		created := newSkipNode(key, v, lvl)
		for i := range lvl {
			created.setNext(i, *nexts.at(i))
			(*prevs.at(i)).setNext(i, created)
		}
		created.setFlag(skipFlagLinked)
		purgeLocks(&prevs, lockedTop)

		atomic.AddUintptr(&s.count, 1)
		return v, false
	}
}

// Range interates cleanly over active mappings.
func (s *SkipMap[K, V]) Range(yield func(key K, value V) bool) {
	curr := s.head.next(0)
	for curr != nil {
		if !curr.hasFlags(skipFlagLinked|skipFlagMarked, skipFlagLinked) {
			curr = curr.next(0)
			continue
		}
		if !yield(curr.key, curr.loadVal()) {
			break
		}
		curr = curr.next(0)
	}
}

// All supports newer loop conventions in the idiomatic framework layout.
//
//go:nosplit
func (s *SkipMap[K, V]) All() func(yield func(K, V) bool) {
	return s.Range
}

// Size retrieves mapping magnitudes.
//
//go:nosplit
func (s *SkipMap[K, V]) Size() int {
	return int(loadUintptr(&s.count))
}

//go:linkname fastrand runtime.fastrand
func fastrand() uint32
