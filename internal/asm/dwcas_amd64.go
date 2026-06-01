//go:build amd64

package asm

import "unsafe"

//go:nosplit
func dwcas(ptr unsafe.Pointer, old1 uint64, old2 unsafe.Pointer, new1 uint64, new2 unsafe.Pointer) bool
