//go:build !race && (amd64 || arm64)

package asm

import (
	"testing"
	"unsafe"
)

func TestDWCAS(t *testing.T) {
	raw := make([]unsafe.Pointer, 3)
	base := unsafe.Pointer(&raw[0])
	if uintptr(base)%16 != 0 {
		base = unsafe.Pointer(&raw[1])
	}

	ptr := (*[2]uintptr)(base)
	ptr[0] = 10
	ptr[1] = 20

	// successful CAS
	ok := DWCAS(base, uintptr(10), unsafe.Pointer(uintptr(20)), uintptr(30), unsafe.Pointer(uintptr(40))) //nolint:all
	if !ok {
		t.Fatalf("expected CAS to succeed")
	}
	if ptr[0] != 30 || ptr[1] != 40 {
		t.Fatalf("expected 30, 40, got %d, %d", ptr[0], ptr[1])
	}

	// failed CAS
	ok = DWCAS(base, uintptr(10), unsafe.Pointer(uintptr(20)), uintptr(50), unsafe.Pointer(uintptr(60))) //nolint:all
	if ok {
		t.Fatalf("expected CAS to fail")
	}
	if ptr[0] != 30 || ptr[1] != 40 {
		t.Fatalf("expected 30, 40, got %d, %d", ptr[0], ptr[1])
	}
}
