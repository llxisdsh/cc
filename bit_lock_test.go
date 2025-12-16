package cc

import (
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
