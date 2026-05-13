//go:build arm64

#include "textflag.h"

// func dwcas(ptr unsafe.Pointer, old1 uintptr, old2 unsafe.Pointer, new1 uintptr, new2 unsafe.Pointer) bool
TEXT ·dwcas(SB), NOSPLIT, $0-49
	MOVD	ptr+0(FP), R0
	MOVD	old1+8(FP), R1
	MOVD	old2+16(FP), R2
	MOVD	new1+24(FP), R3
	MOVD	new2+32(FP), R4

retry:
	LDAXP	(R0), (R5, R6)
	CMP	R1, R5
	BNE	fail
	CMP	R2, R6
	BNE	fail
	STLXP	(R3, R4), (R0), R7
	CBNZ	R7, retry
	MOVD	$1, R8
	MOVB	R8, ret+40(FP)
	RET

fail:
	CLREX
	MOVD	ZR, R8
	MOVB	R8, ret+40(FP)
	RET
