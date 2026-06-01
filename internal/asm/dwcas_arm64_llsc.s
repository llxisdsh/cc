//go:build arm64

#include "textflag.h"

// func dwcasLLSC(ptr unsafe.Pointer, old1 uint64, old2 unsafe.Pointer, new1 uint64, new2 unsafe.Pointer) bool
//
// Uses the ARMv8.0 load/store-exclusive pair instructions instead of LSE
// CASPAL so the fast path also runs on non-LSE ARM64 CPUs.
TEXT ·dwcasLLSC(SB), NOSPLIT|NOFRAME, $0-41
	MOVD	ptr+0(FP), R5
	MOVD	old1+8(FP), R0
	MOVD	old2+16(FP), R1
	MOVD	new1+24(FP), R2
	MOVD	new2+32(FP), R3

retry:
	LDAXP	(R5), (R6, R7)
	CMP	R0, R6
	BNE	fail
	CMP	R1, R7
	BNE	fail
	STLXP	(R2, R3), (R5), R8
	CBNZ	R8, retry
	MOVD	$1, R0
	MOVB	R0, ret+40(FP)
	RET

fail:
	CLREX
	MOVD	$0, R0
	MOVB	R0, ret+40(FP)
	RET
