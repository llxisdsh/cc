//go:build amd64

#include "textflag.h"

// func dwcas(ptr unsafe.Pointer, old1, old2, new1, new2 uintptr) bool
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
