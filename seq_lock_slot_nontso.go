//go:build !(amd64 || 386 || s390x)

package cc

import (
	"sync/atomic"
	"unsafe"
)

// ReadUnfenced copies the value from the slot.
// It uses atomic loads when alignment and size permit to avoid reordering on
// weak memory models. Must be called within a SeqLock read transaction or under
// a lock.
//
//go:nosplit
func (slot *SeqLockSlot[T]) ReadUnfenced() (v T) {
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
		case 5:
			p := (*[5]uintptr)(unsafe.Pointer(&slot.buf))
			q := (*[5]uintptr)(unsafe.Pointer(&v))
			q[0] = atomic.LoadUintptr(&p[0])
			q[1] = atomic.LoadUintptr(&p[1])
			q[2] = atomic.LoadUintptr(&p[2])
			q[3] = atomic.LoadUintptr(&p[3])
			q[4] = atomic.LoadUintptr(&p[4])
		case 6:
			p := (*[6]uintptr)(unsafe.Pointer(&slot.buf))
			q := (*[6]uintptr)(unsafe.Pointer(&v))
			q[0] = atomic.LoadUintptr(&p[0])
			q[1] = atomic.LoadUintptr(&p[1])
			q[2] = atomic.LoadUintptr(&p[2])
			q[3] = atomic.LoadUintptr(&p[3])
			q[4] = atomic.LoadUintptr(&p[4])
			q[5] = atomic.LoadUintptr(&p[5])
		case 7:
			p := (*[7]uintptr)(unsafe.Pointer(&slot.buf))
			q := (*[7]uintptr)(unsafe.Pointer(&v))
			q[0] = atomic.LoadUintptr(&p[0])
			q[1] = atomic.LoadUintptr(&p[1])
			q[2] = atomic.LoadUintptr(&p[2])
			q[3] = atomic.LoadUintptr(&p[3])
			q[4] = atomic.LoadUintptr(&p[4])
			q[5] = atomic.LoadUintptr(&p[5])
			q[6] = atomic.LoadUintptr(&p[6])
		case 8:
			p := (*[8]uintptr)(unsafe.Pointer(&slot.buf))
			q := (*[8]uintptr)(unsafe.Pointer(&v))
			q[0] = atomic.LoadUintptr(&p[0])
			q[1] = atomic.LoadUintptr(&p[1])
			q[2] = atomic.LoadUintptr(&p[2])
			q[3] = atomic.LoadUintptr(&p[3])
			q[4] = atomic.LoadUintptr(&p[4])
			q[5] = atomic.LoadUintptr(&p[5])
			q[6] = atomic.LoadUintptr(&p[6])
			q[7] = atomic.LoadUintptr(&p[7])
		default:
			src := unsafe.Pointer(&slot.buf)
			dst := unsafe.Pointer(&v)
			for i := range n {
				offset := i * ws
				*(*uintptr)(unsafe.Add(dst, offset)) = atomic.LoadUintptr((*uintptr)(unsafe.Add(src, offset)))
			}
		}
		return v
	}
	return slot.buf
}
