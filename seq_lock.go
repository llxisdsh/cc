package cc

import (
	"sync/atomic"
	"unsafe"

	"github.com/llxisdsh/cc/internal/opt"
)

// SeqLock implements a sequence lock (seqlock) for tear-free reads of a single slot.
//
// It is optimized for read-heavy scenarios where readers should not block writers,
// and consistent snapshots are required.
//
// Usage:
//
//	Pair with SeqLockSlot[T]. Use Read/Write helpers or the Locked
//	variants when an external lock is held.
type SeqLock RWLock

// BeginRead starts a read transaction.
// It returns the current sequence number and true if the lock is free (sequence is even).
//
//go:nosplit
func (l *SeqLock) BeginRead() (s1 uintptr, ok bool) {
	if opt.Race_ {
		(*RWLock)(l).RLock()
		s1 = atomic.LoadUintptr((*uintptr)(l))
		return s1, true
	}

	s1 = atomic.LoadUintptr((*uintptr)(l))
	return s1, s1&1 == 0
}

// EndRead finishes a read transaction.
// It returns true if the sequence number matches s1, indicating a consistent snapshot.
//
//go:nosplit
func (l *SeqLock) EndRead(s1 uintptr) (ok bool) {
	if opt.Race_ {
		(*RWLock)(l).RUnlock()
		return true
	}
	s2 := atomic.LoadUintptr((*uintptr)(l))
	return s1 == s2
}

// BeginWrite starts a write transaction by incrementing the sequence to odd.
// It returns the previous sequence number and true if successful.
//
//go:nosplit
func (l *SeqLock) BeginWrite() (s1 uintptr, ok bool) {
	if opt.Race_ {
		(*RWLock)(l).Lock()
		return 1, true
	}
	s1 = atomic.LoadUintptr((*uintptr)(l))
	if s1&1 != 0 {
		return s1, false
	}
	return s1, atomic.CompareAndSwapUintptr((*uintptr)(l), s1, s1|1)
}

// EndWrite finishes a write transaction by incrementing the sequence to even.
//
//go:nosplit
func (l *SeqLock) EndWrite(s1 uintptr) {
	if opt.Race_ {
		(*RWLock)(l).Unlock()
		return
	}
	atomic.StoreUintptr((*uintptr)(l), s1+2)
}

// BeginWriteLocked starts a write transaction assuming an external lock is held.
//
//go:nosplit
func (l *SeqLock) BeginWriteLocked() {
	if opt.Race_ {
		(*RWLock)(l).Lock()
		return
	}
	atomic.AddUintptr((*uintptr)(l), 1)
}

// EndWriteLocked finishes a write transaction assuming an external lock is held.
//
//go:nosplit
func (l *SeqLock) EndWriteLocked() {
	if opt.Race_ {
		(*RWLock)(l).Unlock()
		return
	}
	atomic.AddUintptr((*uintptr)(l), 1)
}

// ClearLocked resets the lock state.
// Only safe when an external lock is held.
//
//go:nosplit
func (l *SeqLock) ClearLocked() {
	atomic.StoreUintptr((*uintptr)(l), 0)
}

// Ready reports whether the lock has been unlocked at least once and is currently free.
// It is useful for checking if the protected data has been initialized.
//
//go:nosplit
func (l *SeqLock) Ready() bool {
	return (*RWLock)(l).Ready()
}

// SeqLock32 is a 32-bit sequence lock.
type SeqLock32 RWLock32

// BeginRead starts a read transaction.
// It returns the current sequence number and true if the lock is free (sequence is even).
//
//go:nosplit
func (l *SeqLock32) BeginRead() (s1 uint32, ok bool) {
	if opt.Race_ {
		(*RWLock32)(l).RLock()
		s1 = atomic.LoadUint32((*uint32)(l))
		return s1, true
	}
	s1 = atomic.LoadUint32((*uint32)(l))
	return s1, s1&1 == 0
}

// EndRead finishes a read transaction.
// It returns true if the sequence number matches s1, indicating a consistent snapshot.
//
//go:nosplit
func (l *SeqLock32) EndRead(s1 uint32) (ok bool) {
	if opt.Race_ {
		(*RWLock32)(l).RUnlock()
		return true
	}
	s2 := atomic.LoadUint32((*uint32)(l))
	return s1 == s2
}

// BeginWrite starts a write transaction by incrementing the sequence to odd.
// It returns the previous sequence number and true if successful.
//
//go:nosplit
func (l *SeqLock32) BeginWrite() (s1 uint32, ok bool) {
	if opt.Race_ {
		(*RWLock32)(l).Lock()
		return 1, true
	}
	s1 = atomic.LoadUint32((*uint32)(l))
	if s1&1 != 0 {
		return s1, false
	}
	return s1, atomic.CompareAndSwapUint32((*uint32)(l), s1, s1|1)
}

// EndWrite finishes a write transaction by incrementing the sequence to even.
//
//go:nosplit
func (l *SeqLock32) EndWrite(s1 uint32) {
	if opt.Race_ {
		(*RWLock32)(l).Unlock()
		return
	}
	atomic.StoreUint32((*uint32)(l), s1+2)
}

// BeginWriteLocked starts a write transaction assuming an external lock is held.
//
//go:nosplit
func (l *SeqLock32) BeginWriteLocked() {
	if opt.Race_ {
		(*RWLock32)(l).Lock()
		return
	}
	atomic.AddUint32((*uint32)(l), 1)
}

// EndWriteLocked finishes a write transaction assuming an external lock is held.
//
//go:nosplit
func (l *SeqLock32) EndWriteLocked() {
	if opt.Race_ {
		(*RWLock32)(l).Unlock()
		return
	}
	atomic.AddUint32((*uint32)(l), 1)
}

// ClearLocked resets the lock state.
// Only safe when an external lock is held.
//
//go:nosplit
func (l *SeqLock32) ClearLocked() {
	atomic.StoreUint32((*uint32)(l), 0)
}

// Ready reports whether the lock has been unlocked at least once and is currently free.
// It is useful for checking if the protected data has been initialized.
//
//go:nosplit
func (l *SeqLock32) Ready() bool {
	return (*RWLock32)(l).Ready()
}

// SeqLockSlot holds a value protected by a SeqLock.
//
// Copy semantics:
//   - Reads: On TSO (amd64/386/s390x), plain typed copies are sufficient.
//     On weak models, uintptr-sized atomic loads are used inside the stable
//     window when alignment/size permit; otherwise a typed copy is used.
//   - Writes: Always plain typed assignment to preserve GC write barriers
//     and avoid publishing pointers via uintptr/unsafe stores.
//
// Safety:
//   - ReadUnfenced/WriteUnfenced must be called within a SeqLock transaction
//     or under an external lock.
type SeqLockSlot[T any] struct {
	_   [0]atomic.Uintptr
	buf T
}

// ReadUnfenced copies the value from the slot.
// It uses atomic loads on weak memory models if possible to avoid reordering.
// Must be called within a SeqLock read transaction or under a lock.
//
//go:nosplit
func (slot *SeqLockSlot[T]) ReadUnfenced() (v T) {
	if !isTSO_ {
		if unsafe.Sizeof(slot.buf) == 0 {
			return v
		}

		ws := unsafe.Sizeof(uintptr(0))
		sz := unsafe.Sizeof(slot.buf)
		al := unsafe.Alignof(slot.buf)
		if al >= ws && sz%ws == 0 {
			n := sz / ws
			switch n {
			case 1:
				u := atomic.LoadUintptr((*uintptr)(unsafe.Pointer(&slot.buf)))
				*(*uintptr)(unsafe.Pointer(&v)) = u
			case 2:
				p := (*[2]uintptr)(unsafe.Pointer(&slot.buf))
				q := (*[2]uintptr)(unsafe.Pointer(&v))
				q[0] = atomic.LoadUintptr(&p[0])
				q[1] = atomic.LoadUintptr(&p[1])
			case 3:
				p := (*[3]uintptr)(unsafe.Pointer(&slot.buf))
				q := (*[3]uintptr)(unsafe.Pointer(&v))
				q[0] = atomic.LoadUintptr(&p[0])
				q[1] = atomic.LoadUintptr(&p[1])
				q[2] = atomic.LoadUintptr(&p[2])
			case 4:
				p := (*[4]uintptr)(unsafe.Pointer(&slot.buf))
				q := (*[4]uintptr)(unsafe.Pointer(&v))
				q[0] = atomic.LoadUintptr(&p[0])
				q[1] = atomic.LoadUintptr(&p[1])
				q[2] = atomic.LoadUintptr(&p[2])
				q[3] = atomic.LoadUintptr(&p[3])
			default:
				for i := range n {
					off := i * ws
					src := (*uintptr)(unsafe.Add(unsafe.Pointer(&slot.buf), off))
					dst := (*uintptr)(unsafe.Add(unsafe.Pointer(&v), off))
					*dst = atomic.LoadUintptr(src)
				}
			}
			return v
		}
	}
	return slot.buf
}

// WriteUnfenced writes v into buf via a plain typed assignment.
// This preserves Go's GC write barriers and avoids publishing pointers
// through uintptr/unsafe atomic stores.
// Must be called under a lock or within a seqlock-stable window.
//
//go:nosplit
func (slot *SeqLockSlot[T]) WriteUnfenced(v T) {
	slot.buf = v
}

// Ptr returns the address of the inline buffer.
// Mutations through this pointer must be guarded by an external lock or
// odd/even sequence; otherwise readers may observe torn data.
//
//go:nosplit
func (slot *SeqLockSlot[T]) Ptr() *T {
	return &slot.buf
}

// SeqLockRead atomically loads a tear-free snapshot using the external SeqLock.
// Spins until seq is even and unchanged across two reads; copies the value
// within the stable window.
func SeqLockRead[T any](sl *SeqLock, slot *SeqLockSlot[T]) (v T) {
	if s1, ok := sl.BeginRead(); ok {
		v = slot.ReadUnfenced()
		if ok = sl.EndRead(s1); ok {
			return v
		}
	}
	return slowSeqLockRead(sl, slot)
}

func slowSeqLockRead[T any](sl *SeqLock, slot *SeqLockSlot[T]) (v T) {
	var spins int
	for {
		if s1, ok := sl.BeginRead(); ok {
			v = slot.ReadUnfenced()
			if ok = sl.EndRead(s1); ok {
				return v
			}
			continue
		}
		delay(&spins)
	}
}

// SeqLockWrite publishes v guarded by the external SeqLock.
// Enters odd, copies v, then exits to even to publish a stable snapshot.
func SeqLockWrite[T any](sl *SeqLock, slot *SeqLockSlot[T], v T) {
	if s1, ok := sl.BeginWrite(); ok {
		slot.WriteUnfenced(v)
		sl.EndWrite(s1)
		return
	}
	slowSeqLockWrite(sl, slot, v)
}

func slowSeqLockWrite[T any](sl *SeqLock, slot *SeqLockSlot[T], v T) {
	var spins int
	for {
		if s1, ok := sl.BeginWrite(); ok {
			slot.WriteUnfenced(v)
			sl.EndWrite(s1)
			return
		}
		delay(&spins)
	}
}

// SeqLockRead32 atomically loads a tear-free snapshot using the external SeqLock.
// Spins until seq is even and unchanged across two reads; copies the value
// within the stable window.
func SeqLockRead32[T any](sl *SeqLock32, slot *SeqLockSlot[T]) (v T) {
	if s1, ok := sl.BeginRead(); ok {
		v = slot.ReadUnfenced()
		if ok = sl.EndRead(s1); ok {
			return v
		}
	}
	return slowSeqLockRead32(sl, slot)
}

func slowSeqLockRead32[T any](sl *SeqLock32, slot *SeqLockSlot[T]) (v T) {
	var spins int
	for {
		if s1, ok := sl.BeginRead(); ok {
			v = slot.ReadUnfenced()
			if ok = sl.EndRead(s1); ok {
				return v
			}
			continue
		}
		delay(&spins)
	}
}

// SeqLockWrite32 publishes v guarded by the external SeqLock.
// Enters odd, copies v, then exits to even to publish a stable snapshot.
func SeqLockWrite32[T any](sl *SeqLock32, slot *SeqLockSlot[T], v T) {
	if s1, ok := sl.BeginWrite(); ok {
		slot.WriteUnfenced(v)
		sl.EndWrite(s1)
		return
	}
	slowSeqLockWrite32(sl, slot, v)
}

func slowSeqLockWrite32[T any](sl *SeqLock32, slot *SeqLockSlot[T], v T) {
	var spins int
	for {
		if s1, ok := sl.BeginWrite(); ok {
			slot.WriteUnfenced(v)
			sl.EndWrite(s1)
			return
		}
		delay(&spins)
	}
}

// SeqLockWriteLocked publishes v assuming an external lock is held.
//
//go:nosplit
func SeqLockWriteLocked[T any](sl *SeqLock, slot *SeqLockSlot[T], v T) {
	sl.BeginWriteLocked()
	slot.WriteUnfenced(v)
	sl.EndWriteLocked()
}

// SeqLockWriteLocked32 publishes v assuming an external lock is held.
//
//go:nosplit
func SeqLockWriteLocked32[T any](sl *SeqLock32, slot *SeqLockSlot[T], v T) {
	sl.BeginWriteLocked()
	slot.WriteUnfenced(v)
	sl.EndWriteLocked()
}
