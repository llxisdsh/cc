//go:build !race && goexperiment.simd && go1.28

package cc

import (
	"math/rand/v2"
	"simd"
	"testing"
	"unsafe"
)

func TestV28SIMDLayout(t *testing.T) {
	width := uintptr(simd.Uint8s{}.Len())
	if v28VectorBytes != width || v28SlotsPerBucket != width-v28CtrlBytes {
		t.Fatalf("layout = (%d, %d), vector bytes = %d", v28VectorBytes, v28SlotsPerBucket, width)
	}
	if want := uint64(1)<<v28SlotsPerBucket - 1; v28LaneMask != want {
		t.Fatalf("lane mask = %#x, want %#x", v28LaneMask, want)
	}
	var bitmap [simd.MaxMaskBitsLen + 1]uint
	for i := range bitmap {
		bitmap[i] = ^uint(0)
	}
	mask := simd.BroadcastUint8s(0).Equal(simd.BroadcastUint8s(1))
	if n := mask.ToBits(bitmap[:]); n != 1 || bitmap[0] != 0 {
		t.Fatalf("empty mask = (%d, %#x), want (1, 0)", n, bitmap[0])
	}
	for _, word := range bitmap[1:] {
		if word != ^uint(0) {
			t.Fatal("ToBits changed a trailing destination word")
		}
	}
	full := simd.BroadcastUint8s(0).Equal(simd.BroadcastUint8s(0))
	full.ToBits(bitmap[:])
	if want := uint(1)<<width - 1; bitmap[0] != want {
		t.Fatalf("full mask = %#x, want %#x", bitmap[0], want)
	}
	t.Logf("vector bytes=%d, tag lanes=%d, emulated=%v", width, v28SlotsPerBucket, simd.Emulated())
}

func TestV28SIMDToBitsShortDestination(t *testing.T) {
	dst := [2]uint{17, 23}
	defer func() {
		if recover() == nil {
			t.Error("ToBits did not panic for a short destination")
		}
		if dst != [2]uint{17, 23} {
			t.Error("ToBits modified a short destination")
		}
	}()
	simd.BroadcastUint8s(0).Equal(simd.BroadcastUint8s(0)).ToBits(dst[:0])
}

func TestV28SIMDMasks(t *testing.T) {
	var data [v28MaxVectorBytes]uint8
	check := func(tag uint8) {
		t.Helper()
		var want [4]uint64
		for i := uintptr(0); i < v28SlotsPerBucket; i++ {
			bit := uint64(1) << i
			if data[i] == tag {
				want[0] |= bit
			}
			if data[i] == v28TagEmpty {
				want[1] |= bit
			}
			if data[i] == v28TagDeleted {
				want[2] |= bit
			}
			if data[i] > v28TagDeleted {
				want[3] |= bit
			}
		}
		bucket := v28Bucket{ptr: unsafe.Pointer(&data[0])}
		got := [4]uint64{v28MatchBits(bucket, tag), v28EmptyBits(bucket), v28DeletedBits(bucket), v28FullBits(bucket)}
		if got != want {
			t.Fatalf("tag=%#x data=%x: masks=%x, want %x", tag, data[:v28VectorBytes], got, want)
		}
	}
	for value := range 256 {
		for i := range data {
			data[i] = uint8(value)
		}
		copy(data[v28SlotsPerBucket:v28VectorBytes], []byte{0, 1, uint8(value), 0xff})
		check(uint8(value))
		check(uint8(value + 1))
	}
	rng := rand.New(rand.NewPCG(1234, 5678))
	for range 4096 {
		tag := uint8(rng.Uint32())
		for i := range data {
			switch rng.IntN(4) {
			case 0:
				data[i] = v28TagEmpty
			case 1:
				data[i] = v28TagDeleted
			case 2:
				data[i] = tag
			default:
				data[i] = uint8(rng.Uint32())
			}
		}
		check(tag)
	}
}

func TestV28MapLoadAcrossFullAndDeletedBuckets(t *testing.T) {
	n := int(v28SlotsPerBucket*3 + 1)
	m := NewV28Map[int, int](WithCapacity(n), WithKeyHasher(func(int, uintptr) uintptr { return 0 }))
	for i := range n {
		m.Store(i, i+1)
	}
	for i := range n {
		if value, ok := m.Load(i); !ok || value != i+1 {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, value, ok, i+1)
		}
	}
	for i := 0; i < n; i += 3 {
		m.Delete(i)
	}
	for i := range n + 1 {
		value, ok := m.Load(i)
		wantOK := i < n && i%3 != 0
		if ok != wantOK || (ok && value != i+1) {
			t.Fatalf("Load(%d) after deletes = (%d, %v), want present=%v", i, value, ok, wantOK)
		}
	}
}

func TestV28MapLoadAllocs(t *testing.T) {
	m := NewV28Map[string, int]()
	m.Store("present", 1)
	allocs := testing.AllocsPerRun(100, func() {
		if value, ok := m.Load("present"); !ok || value != 1 {
			t.Fatal("lost key")
		}
		if _, ok := m.Load("missing"); ok {
			t.Fatal("unexpected hit")
		}
	})
	if allocs != 0 {
		t.Fatalf("Load allocations = %g, want 0", allocs)
	}
}
