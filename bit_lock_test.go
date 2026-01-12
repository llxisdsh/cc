package cc

import (
	"math/bits"
	"sync"
	"testing"
)

func TestBitLockUint64(t *testing.T) {
	var val uint64
	const mask = 1 << 63 // Use highest bit as lock

	var count int
	var wg sync.WaitGroup
	const N = 1000

	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			BitLockUint64(&val, mask)
			count++
			BitUnlockUint64(&val, mask)
		}()
	}
	wg.Wait()

	if count != N {
		t.Errorf("expected count %d, got %d", N, count)
	}
}

func TestBitLockUint32(t *testing.T) {
	var val uint32
	const mask = 1 << 31

	var count int
	var wg sync.WaitGroup
	const N = 1000

	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			BitLockUint32(&val, mask)
			count++
			BitUnlockUint32(&val, mask)
		}()
	}
	wg.Wait()

	if count != N {
		t.Errorf("expected count %d, got %d", N, count)
	}
}

func TestBitLockUintptr(t *testing.T) {
	var val uintptr
	const mask = 1 << (bits.UintSize - 1)

	var count int
	var wg sync.WaitGroup
	const N = 1000

	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			BitLockUintptr(&val, mask)
			count++
			BitUnlockUintptr(&val, mask)
		}()
	}
	wg.Wait()

	if count != N {
		t.Errorf("expected count %d, got %d", N, count)
	}
}

func TestTryBitLockUint64(t *testing.T) {
	var val uint64
	const mask uint64 = 1 << 63

	// TryBitLock should succeed on free lock
	if !BitTryLockUint64(&val, mask) {
		t.Error("BitTryLockUint64 should succeed on free lock")
	}

	// TryBitLock should fail when locked
	if BitTryLockUint64(&val, mask) {
		t.Error("BitTryLockUint64 should fail when locked")
	}

	BitUnlockUint64(&val, mask)
}

func TestTryBitLockUint32(t *testing.T) {
	var val uint32
	const mask uint32 = 1 << 31

	// TryBitLock should succeed on free lock
	if !BitTryLockUint32(&val, mask) {
		t.Error("BitTryLockUint32 should succeed on free lock")
	}

	// TryBitLock should fail when locked
	if BitTryLockUint32(&val, mask) {
		t.Error("BitTryLockUint32 should fail when locked")
	}

	BitUnlockUint32(&val, mask)
}

func TestTryBitLockUintptr(t *testing.T) {
	var val uintptr
	const mask uintptr = 1 << (bits.UintSize - 1)

	// TryBitLock should succeed on free lock
	if !BitTryLockUintptr(&val, mask) {
		t.Error("BitTryLockUintptr should succeed on free lock")
	}

	// TryBitLock should fail when locked
	if BitTryLockUintptr(&val, mask) {
		t.Error("BitTryLockUintptr should fail when locked")
	}

	BitUnlockUintptr(&val, mask)
}
