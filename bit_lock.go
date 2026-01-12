package cc

import "sync/atomic"

// BitLockUint64 acquires a bit-lock on the given address using the specified bit mask.
// It assumes the lock is held if (value & mask) != 0.
// It spins until the lock can be acquired.
//
// This allows embedding a lock bit into an existing uint64 field (e.g., metadata)
// to save memory and avoid false sharing.
func BitLockUint64(addr *uint64, mask uint64) {
	cur := atomic.LoadUint64(addr)
	if atomic.CompareAndSwapUint64(addr, cur&^mask, cur|mask) {
		return
	}
	slowBitLockUint64(addr, mask)
}

func slowBitLockUint64(addr *uint64, mask uint64) {
	var spins int
	for !BitTryLockUint64(addr, mask) {
		delay(&spins)
	}
}

// BitTryLockUint64 attempts to acquire a bit-lock without blocking.
// Returns true if the lock was acquired, false otherwise.
//
//go:nosplit
func BitTryLockUint64(addr *uint64, mask uint64) bool {
	cur := atomic.LoadUint64(addr)
	if cur&mask != 0 {
		return false
	}
	return atomic.CompareAndSwapUint64(addr, cur, cur|mask)
}

// BitUnlockUint64 releases the bit-lock by clearing the specified bit mask.
// It preserves other bits in the value.
//
//go:nosplit
func BitUnlockUint64(addr *uint64, mask uint64) {
	atomic.StoreUint64(addr, loadUint64Fast(addr)&^mask)
}

// BitUnlockWithStoreUint64 releases the bit-lock and simultaneously updates the value.
// It sets the value to (value &^ mask), effectively clearing the lock bit while
// storing new data in the other bits.
//
//go:nosplit
func BitUnlockWithStoreUint64(addr *uint64, mask uint64, value uint64) {
	atomic.StoreUint64(addr, value&^mask)
}

// BitLockUint32 acquires a bit-lock on the given address using the specified bit mask.
func BitLockUint32(addr *uint32, mask uint32) {
	cur := atomic.LoadUint32(addr)
	if atomic.CompareAndSwapUint32(addr, cur&^mask, cur|mask) {
		return
	}
	slowBitLockUint32(addr, mask)
}

func slowBitLockUint32(addr *uint32, mask uint32) {
	var spins int
	for !BitTryLockUint32(addr, mask) {
		delay(&spins)
	}
}

// BitTryLockUint32 attempts to acquire a bit-lock without blocking.
// Returns true if the lock was acquired, false otherwise.
//
//go:nosplit
func BitTryLockUint32(addr *uint32, mask uint32) bool {
	cur := atomic.LoadUint32(addr)
	if cur&mask != 0 {
		return false
	}
	return atomic.CompareAndSwapUint32(addr, cur, cur|mask)
}

// BitUnlockUint32 releases the bit-lock by clearing the specified bit mask.
//
//go:nosplit
func BitUnlockUint32(addr *uint32, mask uint32) {
	atomic.StoreUint32(addr, loadUint32Fast(addr)&^mask)
}

// BitUnlockWithStoreUint32 releases the bit-lock and updates the value.
//
//go:nosplit
func BitUnlockWithStoreUint32(addr *uint32, mask uint32, value uint32) {
	atomic.StoreUint32(addr, value&^mask)
}

// BitLockUintptr acquires a bit-lock on the given address using the specified bit mask.
func BitLockUintptr(addr *uintptr, mask uintptr) {
	cur := atomic.LoadUintptr(addr)
	if atomic.CompareAndSwapUintptr(addr, cur&^mask, cur|mask) {
		return
	}
	slowBitLockUintptr(addr, mask)
}

func slowBitLockUintptr(addr *uintptr, mask uintptr) {
	var spins int
	for !BitTryLockUintptr(addr, mask) {
		delay(&spins)
	}
}

// BitTryLockUintptr attempts to acquire a bit-lock without blocking.
// Returns true if the lock was acquired, false otherwise.
//
//go:nosplit
func BitTryLockUintptr(addr *uintptr, mask uintptr) bool {
	cur := atomic.LoadUintptr(addr)
	if cur&mask != 0 {
		return false
	}
	return atomic.CompareAndSwapUintptr(addr, cur, cur|mask)
}

// BitUnlockUintptr releases the bit-lock by clearing the specified bit mask.
//
//go:nosplit
func BitUnlockUintptr(addr *uintptr, mask uintptr) {
	atomic.StoreUintptr(addr, loadUintptrFast(addr)&^mask)
}

// BitUnlockWithStoreUintptr releases the bit-lock and updates the value.
//
//go:nosplit
func BitUnlockWithStoreUintptr(addr *uintptr, mask uintptr, value uintptr) {
	atomic.StoreUintptr(addr, value&^mask)
}
