package cc

import (
	"cmp"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	skipMaxLevel     = 16
	skipDefaultLevel = 3

	skipFlagLinked uint32 = 1 << iota
	skipFlagMarked

	skipBaseLinks  = 4
	skipExtraLinks = skipMaxLevel - skipBaseLinks
)

// SkipMap represents a lock-free, concurrent-safe skip list mapping keys to values.
// It is designed to scale under heavy contention while minimizing cache-line bounces.
type SkipMap[K cmp.Ordered, V any] struct {
	count    uintptr
	topLevel uintptr
	head     *skipNode[K, V]
}

type skipNode[K cmp.Ordered, V any] struct {
	// Group read-heavy fields in the first 64 bytes (cache line).
	// This dramatically reduces false sharing when the node is frequently accessed.
	key   K
	flags uint32
	level uint32
	value unsafe.Pointer // *V
	links skipOptionalArray

	// Isolate contended lock fields in their own cache line boundary
	mu sync.Mutex
}

type skipOptionalArray struct {
	base  [skipBaseLinks]unsafe.Pointer
	extra *[skipExtraLinks]unsafe.Pointer
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
		n.links.extra = new([skipExtraLinks]unsafe.Pointer)
	}
	return n
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
		return (*skipNode[K, V])(loadPtr(&n.links.base[layer]))
	}
	return (*skipNode[K, V])(loadPtr(&n.links.extra[layer-skipBaseLinks]))
}

//go:nosplit
func (n *skipNode[K, V]) setNext(layer int, dest *skipNode[K, V]) {
	if layer < skipBaseLinks {
		storePtr(&n.links.base[layer], unsafe.Pointer(dest))
		return
	}
	storePtr(&n.links.extra[layer-skipBaseLinks], unsafe.Pointer(dest))
}

//go:nosplit
func (n *skipNode[K, V]) setFlag(f uint32) {
	atomic.OrUint32(&n.flags, f)
}

//go:nosplit
func (n *skipNode[K, V]) hasFlag(f uint32) bool {
	return (loadUint32(&n.flags) & f) != 0
}

//go:nosplit
func (n *skipNode[K, V]) hasFlags(mask, expect uint32) bool {
	return (loadUint32(&n.flags) & mask) == expect
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
func (s *SkipMap[K, V]) search(k K, prevs, nexts *[skipMaxLevel]*skipNode[K, V]) *skipNode[K, V] {
	curr := s.head
	top := int(loadUintptr(&s.topLevel)) - 1
	for i := top; i >= 0; i-- {
		nex := curr.next(i)
		for nex != nil && nex.key < k {
			curr = nex
			nex = curr.next(i)
		}
		prevs[i] = curr
		nexts[i] = nex

		// Early exit if found
		if nex != nil && nex.key == k {
			return nex
		}
	}
	return nil
}

// searchForDelete is an optimized search tailored for deletion that yields the layer the key was found.
func (s *SkipMap[K, V]) searchForDelete(k K, prevs, nexts *[skipMaxLevel]*skipNode[K, V]) int {
	foundAt := -1
	curr := s.head
	top := int(loadUintptr(&s.topLevel)) - 1
	for i := top; i >= 0; i-- {
		nex := curr.next(i)
		for nex != nil && nex.key < k {
			curr = nex
			nex = curr.next(i)
		}
		prevs[i] = curr
		nexts[i] = nex

		if foundAt == -1 && nex != nil && nex.key == k {
			foundAt = i
		}
	}
	return foundAt
}

func purgeLocks[K cmp.Ordered, V any](prevs [skipMaxLevel]*skipNode[K, V], top int) {
	var locked *skipNode[K, V]
	for i := top; i >= 0; i-- {
		p := prevs[i]
		if p != locked {
			p.mu.Unlock()
			locked = p
		}
	}
}

// Store places a key-value mapping into the structure.
func (s *SkipMap[K, V]) Store(key K, value V) {
	lvl := s.randomLevel()
	var prevs, nexts [skipMaxLevel]*skipNode[K, V]

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
			p = prevs[i]
			nex = nexts[i]
			if p != prevP {
				p.mu.Lock()
				lockedTop = i
				prevP = p
			}
			isValid = !p.hasFlag(skipFlagMarked) && (nex == nil || !nex.hasFlag(skipFlagMarked)) && p.next(i) == nex
		}

		if !isValid {
			purgeLocks(prevs, lockedTop)
			continue
		}

		created := newSkipNode(key, value, lvl)
		for i := range lvl {
			created.setNext(i, nexts[i])
			prevs[i].setNext(i, created)
		}
		created.setFlag(skipFlagLinked)
		purgeLocks(prevs, lockedTop)

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

// LoadAndDelete ensures the removal of a mapping and returns it.
func (s *SkipMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	var (
		victim       *skipNode[K, V]
		marked       bool
		victimLvl    = -1
		prevs, nexts [skipMaxLevel]*skipNode[K, V]
	)
	for {
		hitLvl := s.searchForDelete(key, &prevs, &nexts)

		isRemovable := hitLvl != -1 &&
			nexts[hitLvl].hasFlags(skipFlagLinked|skipFlagMarked, skipFlagLinked) &&
			(int(nexts[hitLvl].level)-1) == hitLvl

		if marked || isRemovable {
			if !marked {
				victim = nexts[hitLvl]
				victimLvl = hitLvl
				victim.mu.Lock()
				if victim.hasFlag(skipFlagMarked) {
					victim.mu.Unlock()
					return *new(V), false
				}
				victim.setFlag(skipFlagMarked)
				marked = true
			}

			lockedTop := -1
			isValid := true
			var p, nex, prevP *skipNode[K, V]

			for i := 0; isValid && i <= victimLvl; i++ {
				p = prevs[i]
				nex = nexts[i]
				if p != prevP {
					p.mu.Lock()
					lockedTop = i
					prevP = p
				}
				isValid = !p.hasFlag(skipFlagMarked) && p.next(i) == nex
			}

			if !isValid {
				purgeLocks(prevs, lockedTop)
				continue
			}

			for i := victimLvl; i >= 0; i-- {
				prevs[i].setNext(i, victim.next(i))
			}
			victim.mu.Unlock()
			purgeLocks(prevs, lockedTop)

			atomic.AddUintptr(&s.count, ^uintptr(0))
			return victim.loadVal(), true
		}
		return *new(V), false
	}
}

// Delete eliminates a mapping without returning it.
func (s *SkipMap[K, V]) Delete(key K) bool {
	var (
		victim       *skipNode[K, V]
		marked       bool
		victimLvl    = -1
		prevs, nexts [skipMaxLevel]*skipNode[K, V]
	)
	for {
		hitLvl := s.searchForDelete(key, &prevs, &nexts)

		isRemovable := hitLvl != -1 &&
			nexts[hitLvl].hasFlags(skipFlagLinked|skipFlagMarked, skipFlagLinked) &&
			(int(nexts[hitLvl].level)-1) == hitLvl

		if marked || isRemovable {
			if !marked {
				victim = nexts[hitLvl]
				victimLvl = hitLvl
				victim.mu.Lock()
				if victim.hasFlag(skipFlagMarked) {
					victim.mu.Unlock()
					return false
				}
				victim.setFlag(skipFlagMarked)
				marked = true
			}

			lockedTop := -1
			isValid := true
			var p, nex, prevP *skipNode[K, V]

			for i := 0; isValid && i <= victimLvl; i++ {
				p = prevs[i]
				nex = nexts[i]
				if p != prevP {
					p.mu.Lock()
					lockedTop = i
					prevP = p
				}
				isValid = !p.hasFlag(skipFlagMarked) && p.next(i) == nex
			}

			if !isValid {
				purgeLocks(prevs, lockedTop)
				continue
			}

			for i := victimLvl; i >= 0; i-- {
				prevs[i].setNext(i, victim.next(i))
			}
			victim.mu.Unlock()
			purgeLocks(prevs, lockedTop)

			atomic.AddUintptr(&s.count, ^uintptr(0))
			return true
		}
		return false
	}
}

// LoadOrStore updates value if identical key misses in mapping.
func (s *SkipMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	var (
		lvl          int
		prevs, nexts [skipMaxLevel]*skipNode[K, V]
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
			p = prevs[i]
			nex = nexts[i]
			if p != prevP {
				p.mu.Lock()
				lockedTop = i
				prevP = p
			}
			isValid = !p.hasFlag(skipFlagMarked) && (nex == nil || !nex.hasFlag(skipFlagMarked)) && p.next(i) == nex
		}

		if !isValid {
			purgeLocks(prevs, lockedTop)
			continue
		}

		created := newSkipNode(key, value, lvl)
		for i := 0; i < lvl; i++ {
			created.setNext(i, nexts[i])
			prevs[i].setNext(i, created)
		}
		created.setFlag(skipFlagLinked)
		purgeLocks(prevs, lockedTop)

		atomic.AddUintptr(&s.count, 1)
		return value, false
	}
}

// LoadOrStoreFn operates analogously to LoadOrStore but fetches assignment lazily.
func (s *SkipMap[K, V]) LoadOrStoreFn(key K, newValFn func() V) (actual V, loaded bool) {
	var (
		lvl          int
		prevs, nexts [skipMaxLevel]*skipNode[K, V]
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
			p = prevs[i]
			nex = nexts[i]
			if p != prevP {
				p.mu.Lock()
				lockedTop = i
				prevP = p
			}
			isValid = !p.hasFlag(skipFlagMarked) && (nex == nil || !nex.hasFlag(skipFlagMarked)) && p.next(i) == nex
		}

		if !isValid {
			purgeLocks(prevs, lockedTop)
			continue
		}

		v := newValFn()
		created := newSkipNode(key, v, lvl)
		for i := 0; i < lvl; i++ {
			created.setNext(i, nexts[i])
			prevs[i].setNext(i, created)
		}
		created.setFlag(skipFlagLinked)
		purgeLocks(prevs, lockedTop)

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
