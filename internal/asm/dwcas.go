//go:build amd64 || arm64

package asm

import (
	"structs"
	"unsafe"
)

//go:linkname writeBarrier runtime.writeBarrier
var writeBarrier struct {
	_ structs.HostLayout

	enabled bool
	pad     [3]byte
	alignme uint64
}

// DWCAS atomically compares the 16-byte slot at ptr with (old1, old2).
// On success it publishes (new1, new2). ptr must be 16-byte aligned.
//
// The second word is a Go pointer in DHLTMap slots, so the wrapper performs
// the runtime atomic write barrier before the raw assembly CAS.
//
//go:nosplit
func DWCAS(ptr unsafe.Pointer, old1 uintptr, old2 unsafe.Pointer, new1 uintptr, new2 unsafe.Pointer) bool {
	if writeBarrier.enabled {
		runtime_atomicwb((*unsafe.Pointer)(unsafe.Add(ptr, 8)), new2)
	}
	return dwcasAsm(ptr, old1, old2, new1, new2)
}

//go:nosplit
func dwcasAsm(ptr unsafe.Pointer, old1 uintptr, old2 unsafe.Pointer, new1 uintptr, new2 unsafe.Pointer) bool

//go:linkname runtime_atomicwb runtime.atomicwb
func runtime_atomicwb(ptr *unsafe.Pointer, new unsafe.Pointer)
