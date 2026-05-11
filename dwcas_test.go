package cc

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
	ok := dwcas(base, 10, 20, 30, 40)
	if !ok {
		t.Fatalf("expected CAS to succeed")
	}
	if ptr[0] != 30 || ptr[1] != 40 {
		t.Fatalf("expected 30, 40, got %d, %d", ptr[0], ptr[1])
	}

	// failed CAS
	ok = dwcas(base, 10, 20, 50, 60)
	if ok {
		t.Fatalf("expected CAS to fail")
	}
	if ptr[0] != 30 || ptr[1] != 40 {
		t.Fatalf("expected 30, 40, got %d, %d", ptr[0], ptr[1])
	}
}
