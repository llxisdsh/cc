package cc

import "unsafe"

// dwcas performs a double-word compare-and-swap.
// It atomically compares the 16-byte value at ptr with (old1, old2).
// If they match, it replaces them with (new1, new2) and returns true.
// The ptr must be 16-byte aligned.
func dwcas(ptr unsafe.Pointer, old1 uintptr, old2 unsafe.Pointer, new1 uintptr, new2 unsafe.Pointer) bool
