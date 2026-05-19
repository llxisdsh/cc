//go:build !amd64 && !arm64

package asm

import (
	"unsafe"
)

func DWCAS(ptr unsafe.Pointer, old1 uint64, old2 unsafe.Pointer, new1 uint64, new2 unsafe.Pointer) bool {
	panic("cc: DWCAS fallback not implemented")
}
