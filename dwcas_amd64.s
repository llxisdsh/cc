//go:build amd64

#include "textflag.h"

// func dwcas(ptr unsafe.Pointer, old1 uintptr, old2 unsafe.Pointer, new1 uintptr, new2 unsafe.Pointer) bool
TEXT ·dwcas(SB), NOSPLIT, $0-49
	MOVQ ptr+0(FP), BP
	MOVQ old1+8(FP), AX
	MOVQ old2+16(FP), DX
	MOVQ new1+24(FP), BX
	MOVQ new2+32(FP), CX
	LOCK
	CMPXCHG16B (BP)
	SETEQ ret+40(FP)
	RET
