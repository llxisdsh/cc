//go:build amd64

#include "textflag.h"

// func dwcasAsm(ptr unsafe.Pointer, old1 uint64, old2 unsafe.Pointer, new1 uint64, new2 unsafe.Pointer) bool
TEXT ·dwcasAsm(SB), NOSPLIT|NOFRAME, $0-41
	MOVQ ptr+0(FP), DI
	MOVQ old1+8(FP), AX
	MOVQ old2+16(FP), DX
	MOVQ new1+24(FP), BX
	MOVQ new2+32(FP), CX
	LOCK
	CMPXCHG16B (DI)
	SETEQ ret+40(FP)
	RET
