package cc

import (
	"sync/atomic"
)

// RWLock is a spin-based Reader-Writer lock backed by a uintptr.
// It is writer-preferred to prevent reader starvation.
type RWLock uintptr

const (
	rwWriteLockMask = 1 // bit 0 means writer is holding the lock
	rwWriteMarkMask = 2 // bit 1 means writer has marked the lock
	rwReadShift     = 2 // bits 2+ mean reader count
	rwReadUnit      = 1 << rwReadShift
)

// Lock acquires the write lock.
// It spins until the lock is free.
func (l *RWLock) Lock() {
	s := atomic.LoadUintptr((*uintptr)(l)) & rwWriteMarkMask
	if atomic.CompareAndSwapUintptr((*uintptr)(l), s, s|rwWriteLockMask) {
		return
	}
	l.slowLock()
}

func (l *RWLock) slowLock() {
	var spins int
	for {
		// 1. Acquire Write Bit (Bit 0). This blocks NEW readers.
		s := atomic.LoadUintptr((*uintptr)(l))
		if s&rwWriteLockMask == 0 {
			if atomic.CompareAndSwapUintptr((*uintptr)(l), s, s|rwWriteLockMask) {
				// Acquired Write Bit.
				// 2. Wait for existing Readers to drain.
				// Note: Readers are bits 2+. Bit 1 is the "initialized" bit (value 2).
				// So we wait until s >> rwReadShift == 0.
				for {
					s2 := atomic.LoadUintptr((*uintptr)(l))
					if s2>>rwReadShift == 0 {
						return
					}
					delay(&spins)
				}
			}
		}
		delay(&spins)
	}
}

// Unlock releases the write lock.
// It resets the state to rwWriteCountMask (2), indicating "initialized and free".
func (l *RWLock) Unlock() {
	atomic.StoreUintptr((*uintptr)(l), rwWriteMarkMask)
}

// Ready reports whether the lock has been unlocked at least once and is currently free.
// It is useful for checking if the protected data has been initialized.
func (l *RWLock) Ready() bool {
	s := atomic.LoadUintptr((*uintptr)(l))
	return s != 0 && s&rwWriteLockMask == 0
}

// RLock acquires a read lock.
func (l *RWLock) RLock() {
	s := atomic.LoadUintptr((*uintptr)(l)) &^ rwWriteLockMask
	if atomic.CompareAndSwapUintptr((*uintptr)(l), s, s+rwReadUnit) {
		return
	}
	l.slowRLock()
}

func (l *RWLock) slowRLock() {
	var spins int
	for {
		s := atomic.LoadUintptr((*uintptr)(l))
		if s&rwWriteLockMask == 0 { // No writer
			if atomic.CompareAndSwapUintptr((*uintptr)(l), s, s+rwReadUnit) {
				return
			}
		}
		delay(&spins)
	}
}

// RUnlock releases a read lock.
func (l *RWLock) RUnlock() {
	atomic.AddUintptr((*uintptr)(l), ^uintptr(rwReadUnit-1))
}

// RWLock32 is a spin-based Reader-Writer lock backed by a uint32.
// It is writer-preferred to prevent reader starvation.
type RWLock32 uint32

// Lock acquires the write lock.
func (l *RWLock32) Lock() {
	s := atomic.LoadUint32((*uint32)(l)) & rwWriteMarkMask
	if atomic.CompareAndSwapUint32((*uint32)(l), s, s|rwWriteLockMask) {
		return
	}
	l.slowLock()
}

func (l *RWLock32) slowLock() {
	var spins int
	for {
		s := atomic.LoadUint32((*uint32)(l))
		if s&rwWriteLockMask == 0 {
			if atomic.CompareAndSwapUint32((*uint32)(l), s, s|rwWriteLockMask) {
				for {
					s2 := atomic.LoadUint32((*uint32)(l))
					if s2>>rwReadShift == 0 {
						return
					}
					delay(&spins)
				}
			}
		}
		delay(&spins)
	}
}

// Unlock releases the write lock.
func (l *RWLock32) Unlock() {
	atomic.StoreUint32((*uint32)(l), rwWriteMarkMask)
}

// RLock acquires a read lock.
func (l *RWLock32) RLock() {
	s := atomic.LoadUint32((*uint32)(l)) &^ rwWriteLockMask
	if atomic.CompareAndSwapUint32((*uint32)(l), s, s+rwReadUnit) {
		return
	}
	l.slowRLock()
}

func (l *RWLock32) slowRLock() {
	var spins int
	for {
		s := atomic.LoadUint32((*uint32)(l))
		if s&rwWriteLockMask == 0 {
			if atomic.CompareAndSwapUint32((*uint32)(l), s, s+rwReadUnit) {
				return
			}
		}
		delay(&spins)
	}
}

// RUnlock releases a read lock.
func (l *RWLock32) RUnlock() {
	atomic.AddUint32((*uint32)(l), ^uint32(rwReadUnit-1))
}

// Ready reports whether the lock has been unlocked at least once and is currently free.
// It is useful for checking if the protected data has been initialized.
func (l *RWLock32) Ready() bool {
	s := atomic.LoadUint32((*uint32)(l))
	return s != 0 && s&rwWriteLockMask == 0
}
