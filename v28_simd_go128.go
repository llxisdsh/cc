//go:build !race && goexperiment.simd && go1.28

package cc

import (
	"math/bits"
	"simd"
	"unsafe"
)

func init() {
	v28InitSIMDLayout()
}

func v28InitSIMDLayout() {
	vectorBytes := uintptr(simd.Uint8s{}.Len())
	// Alignment requires a power of two; probing currently consumes one word.
	if vectorBytes < v28MinVectorBytes || vectorBytes > v28MaxVectorBytes ||
		vectorBytes&(vectorBytes-1) != 0 || vectorBytes > uintptr(bits.UintSize) {
		panic("cc: unsupported V28 SIMD vector size")
	}
	v28VectorBytes = vectorBytes
	v28SlotsPerBucket = vectorBytes - v28CtrlBytes
	v28LaneMask = uint64(1)<<v28SlotsPerBucket - 1
}

func v28MatchBits(b v28Bucket, tag uint8) uint64 {
	var bitmap [simd.MaxMaskBitsLen]uint
	words := simd.LoadUint8s(unsafe.Slice((*uint8)(b.ptr), simd.Uint8s{}.Len()))
	words.Equal(simd.BroadcastUint8s(tag)).ToBits(bitmap[:])
	return uint64(bitmap[0]) & v28LaneMask
}

func v28EmptyBits(b v28Bucket) uint64 {
	var bitmap [simd.MaxMaskBitsLen]uint
	words := simd.LoadUint8s(unsafe.Slice((*uint8)(b.ptr), simd.Uint8s{}.Len()))
	words.Equal(simd.BroadcastUint8s(v28TagEmpty)).ToBits(bitmap[:])
	return uint64(bitmap[0]) & v28LaneMask
}

func v28DeletedBits(b v28Bucket) uint64 {
	var bitmap [simd.MaxMaskBitsLen]uint
	words := simd.LoadUint8s(unsafe.Slice((*uint8)(b.ptr), simd.Uint8s{}.Len()))
	words.Equal(simd.BroadcastUint8s(v28TagDeleted)).ToBits(bitmap[:])
	return uint64(bitmap[0]) & v28LaneMask
}

// v28InsertLane prefers a deleted lane in the same bucket snapshot when the
// bucket also has an empty lane, so absence is still proven by that snapshot.
// Reusing deleted lanes from earlier buckets would need a cross-bucket proof
// under concurrent delete/insert races.
func v28InsertLane(b v28Bucket, empty uint64) (uintptr, bool) {
	if v28EnableTerminalTombstoneReuse {
		if deleted := v28DeletedBits(b); deleted != 0 {
			return uintptr(bits.TrailingZeros64(deleted)), true
		}
	}
	return uintptr(bits.TrailingZeros64(empty)), false
}

func v28FullBits(b v28Bucket) uint64 {
	var bitmap [simd.MaxMaskBitsLen]uint
	words := simd.LoadUint8s(unsafe.Slice((*uint8)(b.ptr), simd.Uint8s{}.Len()))
	// Portable Uint8s has no Greater: x > 1 is equivalent to (x & 0xfe) != 0.
	words.And(simd.BroadcastUint8s(0xfe)).NotEqual(simd.BroadcastUint8s(0)).ToBits(bitmap[:])
	return uint64(bitmap[0]) & v28LaneMask
}
