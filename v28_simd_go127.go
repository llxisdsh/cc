//go:build !race && amd64 && goexperiment.simd && go1.27

package cc

import (
	"simd/archsimd"
	"unsafe"
)

//go:nosplit
func v28LoadTagWords(b *v28Bucket) archsimd.Uint8x32 {
	// Go 1.27 split the fixed-array loader out from the slice loader. Use the
	// array form so this hot path does not construct a slice header or pay the
	// slice length check.
	//
	// V28 buckets are exactly one 32-byte SIMD group: 28 tag bytes followed by
	// ctrl. The ctrl lanes are masked off by v28LaneMask after each comparison.
	return archsimd.LoadUint8x32Array((*[32]uint8)(unsafe.Pointer(&b.tags[0])))
}
