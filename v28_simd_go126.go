//go:build !race && amd64 && goexperiment.simd && !go1.27

package cc

import (
	"simd/archsimd"
	"unsafe"
)

//go:nosplit
func v28LoadTagWords(b *v28Bucket) archsimd.Uint8x32 {
	// Go 1.26 archsimd.LoadUint8x32 accepts a fixed 32-byte array pointer.
	// V28 buckets are exactly one 32-byte SIMD group: 28 tag bytes followed by
	// ctrl. The ctrl lanes are masked off by v28LaneMask after each comparison.
	return archsimd.LoadUint8x32((*[32]uint8)(unsafe.Pointer(&b.tags[0])))
}
