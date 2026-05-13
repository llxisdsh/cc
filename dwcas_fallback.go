//go:build !amd64 && !arm64

package cc

import (
	"sync"
	"unsafe"
)

var dwcasFallbackMu sync.Mutex

// dwcas is a portable fallback for platforms without a native double-word CAS.
// It serializes competing dwcas calls, but it cannot make unrelated plain or
// atomic loads observe the two words as a single hardware transaction.
func dwcas(ptr unsafe.Pointer, old1 uintptr, old2 unsafe.Pointer, new1 uintptr, new2 unsafe.Pointer) bool {
	words := (*[2]uintptr)(ptr)
	ptrs := (*[2]unsafe.Pointer)(ptr)

	dwcasFallbackMu.Lock()
	ok := words[0] == old1 && ptrs[1] == old2
	if ok {
		words[0] = new1
		ptrs[1] = new2
	}
	dwcasFallbackMu.Unlock()
	return ok
}
