//go:build arm64

#include "textflag.h"

// func dwcasAsm(ptr unsafe.Pointer, old1 uint64, old2 unsafe.Pointer, new1 uint64, new2 unsafe.Pointer) bool
//
// Uses ARMv8.1-A LSE CASPAL for maximum throughput. This experimental map
// intentionally targets newer ARM64 CPUs; non-LSE ARMv8.0 hardware is not
// supported by this fast path.
TEXT ·dwcasAsm(SB), NOSPLIT|NOFRAME, $0-41
	MOVD	ptr+0(FP), R5
	MOVD	old1+8(FP), R0
	MOVD	old2+16(FP), R1
	MOVD	new1+24(FP), R2
	MOVD	new2+32(FP), R3
	MOVD	R0, R6
	MOVD	R1, R7
	WORD	$0x4820fca2 // CASPAL (R0, R1), (R5), (R2, R3)
	CMP	R0, R6
	CCMP	EQ, R1, R7, $0
	CSET	EQ, R0
	MOVB	R0, ret+40(FP)
	RET
