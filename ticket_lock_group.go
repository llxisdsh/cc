package cc

// TicketLockGroup allows locking on arbitrary keys (string, int, struct, etc.).
// It dynamically manages a set of locks associated with values.
//
// Features:
//   - Infinite Keys: No need to pre-allocate locks.
//   - Auto-Cleanup: Locks are automatically removed from memory when unlocked and no one else is waiting.
//   - Low Overhead: Uses a sharded map structure internally for concurrent access.
//
// Usage:
//
//	var group TicketLockGroup[string]
//	group.Lock("user-123")
//	// Critical section for user-123
//	group.Unlock("user-123")
//
// Implementation Note:
// It uses reference counting to safely delete entries.
type TicketLockGroup[K comparable] struct {
	_ noCopy
	m Map[K, *lockGroupEntry]
}

type lockGroupEntry struct {
	mu  TicketLock
	ref int32
}

// Lock acquires the lock for the given key.
func (g *TicketLockGroup[K]) Lock(k K) {
	v, _ := g.m.Compute(k, func(e *MapEntry[K, *lockGroupEntry]) {
		val := e.Value()
		if val == nil {
			val = &lockGroupEntry{}
		}
		val.ref++
		e.Update(val)
	})
	v.mu.Lock()
}

// Unlock releases the lock for the given key.
func (g *TicketLockGroup[K]) Unlock(k K) {
	v, ok := g.m.Load(k)
	if !ok {
		return
	}
	v.mu.Unlock()

	g.m.Compute(k, func(e *MapEntry[K, *lockGroupEntry]) {
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
