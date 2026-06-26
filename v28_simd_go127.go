//go:build !race && amd64 && goexperiment.simd && go1.27

package cc

import (
	"math/bits"
	"os"
	"simd/archsimd"
	"strconv"
	"strings"
)

// The Go 1.27 portable simd package is the intended long-term backend, but
// go1.27rc1 currently fails to compile portable simd vector types in build-tag
// gated files and generic packages. Keep this backend on archsimd for now while
// preserving the same 128/256/512-byte-layout boundary in V28.
func init() {
	v28InitSIMDLayout()
}

func v28InitSIMDLayout() {
	vectorBytes := uintptr(v28MinVectorBytes)
	if archsimd.X86.AVX512() {
		vectorBytes = 64
	} else if archsimd.X86.AVX2() {
		vectorBytes = 32
	}
	if vectorBytes > v28MaxVectorBytes {
		vectorBytes = v28MaxVectorBytes
	}
	if debugBytes := v28DebugSIMDBytes(); debugBytes != 0 && debugBytes < vectorBytes {
		vectorBytes = debugBytes
	}
	v28VectorBytes = vectorBytes
	v28SlotsPerBucket = vectorBytes - v28CtrlBytes
	v28LaneMask = uint64(1)<<v28SlotsPerBucket - 1
}

func v28DebugSIMDBytes() uintptr {
	// Mirror portable simd's GODEBUG=simd=N reduction knob. This only lowers the
	// selected width; it never forces a wider vector than the CPU path selected.
	for _, field := range strings.Split(os.Getenv("GODEBUG"), ",") {
		key, value, ok := strings.Cut(field, "=")
		if !ok || key != "simd" {
			continue
		}
		bits, err := strconv.Atoi(value)
		if err != nil {
			return 0
		}
		switch bits {
		case 128, 256, 512:
			return uintptr(bits / 8)
		default:
			return 0
		}
	}
	return 0
}

func v28MatchBits(b v28Bucket, tag uint8) uint64 {
	switch v28VectorBytes {
	case 64:
		words := archsimd.LoadUint8x64Array((*[64]uint8)(b.ptr))
		return words.Equal(archsimd.BroadcastUint8x64(tag)).ToBits() & v28LaneMask
	case 32:
		words := archsimd.LoadUint8x32Array((*[32]uint8)(b.ptr))
		return uint64(words.Equal(archsimd.BroadcastUint8x32(tag)).ToBits()) & v28LaneMask
	default:
		words := archsimd.LoadUint8x16Array((*[16]uint8)(b.ptr))
		return uint64(words.Equal(archsimd.BroadcastUint8x16(tag)).ToBits()) & v28LaneMask
	}
}

func v28EmptyBits(b v28Bucket) uint64 {
	switch v28VectorBytes {
	case 64:
		words := archsimd.LoadUint8x64Array((*[64]uint8)(b.ptr))
		return words.Equal(archsimd.BroadcastUint8x64(v28TagEmpty)).ToBits() & v28LaneMask
	case 32:
		words := archsimd.LoadUint8x32Array((*[32]uint8)(b.ptr))
		return uint64(words.Equal(archsimd.BroadcastUint8x32(v28TagEmpty)).ToBits()) & v28LaneMask
	default:
		words := archsimd.LoadUint8x16Array((*[16]uint8)(b.ptr))
		return uint64(words.Equal(archsimd.BroadcastUint8x16(v28TagEmpty)).ToBits()) & v28LaneMask
	}
}

func v28DeletedBits(b v28Bucket) uint64 {
	switch v28VectorBytes {
	case 64:
		words := archsimd.LoadUint8x64Array((*[64]uint8)(b.ptr))
		return words.Equal(archsimd.BroadcastUint8x64(v28TagDeleted)).ToBits() & v28LaneMask
	case 32:
		words := archsimd.LoadUint8x32Array((*[32]uint8)(b.ptr))
		return uint64(words.Equal(archsimd.BroadcastUint8x32(v28TagDeleted)).ToBits()) & v28LaneMask
	default:
		words := archsimd.LoadUint8x16Array((*[16]uint8)(b.ptr))
		return uint64(words.Equal(archsimd.BroadcastUint8x16(v28TagDeleted)).ToBits()) & v28LaneMask
	}
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
	switch v28VectorBytes {
	case 64:
		words := archsimd.LoadUint8x64Array((*[64]uint8)(b.ptr))
		return words.Greater(archsimd.BroadcastUint8x64(v28TagDeleted)).ToBits() & v28LaneMask
	case 32:
		words := archsimd.LoadUint8x32Array((*[32]uint8)(b.ptr))
		return uint64(words.Greater(archsimd.BroadcastUint8x32(v28TagDeleted)).ToBits()) & v28LaneMask
	default:
		words := archsimd.LoadUint8x16Array((*[16]uint8)(b.ptr))
		return uint64(words.Greater(archsimd.BroadcastUint8x16(v28TagDeleted)).ToBits()) & v28LaneMask
	}
}
