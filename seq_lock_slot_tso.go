//go:build amd64 || 386 || s390x

package cc

// ReadUnfenced copies the value from the slot.
// Must be called within a SeqLock read transaction or under a lock.
//
//go:nosplit
func (slot *SeqLockSlot[T]) ReadUnfenced() (v T) {
	return slot.buf
}
