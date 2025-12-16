package cc

// RWLockGroup allows shared Reader-Writer locking on arbitrary keys.
// It matches the interface of LockGroup but supports RLock/RUnlock.
//
// Features:
//   - RLock/RUnlock for shared read access.
//   - Lock/Unlock for exclusive write access.
//   - Infinite Keys & Auto-Cleanup.
//
// Usage:
//
//	var group RWLockGroup[string]
//
//	// Readers
//	group.RLock("config")
//	read(config)
//	group.RUnlock("config")
//
//	// Writer
//	group.Lock("config")
//	write(config)
//	group.Unlock("config")
type RWLockGroup[K comparable] struct {
	_ noCopy
	m Map[K, *rwLockGroupEntry]
}

type rwLockGroupEntry struct {
	mu  RWLock
	ref int32
}

// Lock acquires an exclusive write lock for the given key.
func (g *RWLockGroup[K]) Lock(k K) {
	v, _ := g.m.Compute(k, func(e *Entry[K, *rwLockGroupEntry]) {
		val := e.Value()
		if val == nil {
			val = &rwLockGroupEntry{}
		}
		val.ref++
		e.Update(val)
	})
	v.mu.Lock()
}

// Unlock releases the write lock for the given key.
func (g *RWLockGroup[K]) Unlock(k K) {
	v, ok := g.m.Load(k)
	if !ok {
		return
	}
	v.mu.Unlock()

	g.m.Compute(k, func(e *Entry[K, *rwLockGroupEntry]) {
		val := e.Value()
		if val == nil {
			return
		}
		val.ref--
		if val.ref <= 0 {
			e.Delete()
		}
	})
}

// RLock acquires a shared read lock for the given key.
func (g *RWLockGroup[K]) RLock(k K) {
	v, _ := g.m.Compute(k, func(e *Entry[K, *rwLockGroupEntry]) {
		val := e.Value()
		if val == nil {
			val = &rwLockGroupEntry{}
		}
		val.ref++
		e.Update(val)
	})
	v.mu.RLock()
}

// RUnlock releases the read lock for the given key.
func (g *RWLockGroup[K]) RUnlock(k K) {
	v, ok := g.m.Load(k)
	if !ok {
		return
	}
	v.mu.RUnlock()

	g.m.Compute(k, func(e *Entry[K, *rwLockGroupEntry]) {
		val := e.Value()
		if val == nil {
			return
		}
		val.ref--
		if val.ref <= 0 {
			e.Delete()
		}
	})
}
