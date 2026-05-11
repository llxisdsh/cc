package cc

import "unsafe"

// dwcas performs a double-word compare-and-swap.
// It atomically compares the 16-byte value at ptr with (old1, old2).
// If they match, it replaces them with (new1, new2) and returns true.
// The ptr must be 16-byte aligned.
func dwcas(ptr unsafe.Pointer, old1, old2, new1, new2 uintptr) bool

//nolint:unused
var escapeSink unsafe.Pointer

//go:nosplit
func escape(x unsafe.Pointer) {
	escapeSink = x
}
