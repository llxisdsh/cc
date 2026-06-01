//go:build arm64

package asm

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

var hasLSE = cpu.ARM64.HasATOMICS

//go:nosplit
func dwcas(ptr unsafe.Pointer, old1 uint64, old2 unsafe.Pointer, new1 uint64, new2 unsafe.Pointer) bool {
	if hasLSE {
		return dwcasLSE(ptr, old1, old2, new1, new2)
	}
	return dwcasLLSC(ptr, old1, old2, new1, new2)
}

//go:nosplit
func dwcasLSE(ptr unsafe.Pointer, old1 uint64, old2 unsafe.Pointer, new1 uint64, new2 unsafe.Pointer) bool

//go:nosplit
func dwcasLLSC(ptr unsafe.Pointer, old1 uint64, old2 unsafe.Pointer, new1 uint64, new2 unsafe.Pointer) bool
