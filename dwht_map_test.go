//go:build !race && (amd64 || arm64)

package cc

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestDWHTSlotStorageLayout(t *testing.T) {
	idx := bits.TrailingZeros(uint(dwhtMinSlots))
	hint := &dwhtSlotLayoutHints[idx]
	hint.Store(dwhtSlotLayoutUnknown)

	table := newDWHTTable[int, int](128, uintptr(dwhtMaxProbeThreshold))
	if uintptr(table.slotsBase)&(dwhtSlotBytes-1) != 0 {
		t.Fatalf("slotsBase=%#x is not %d-byte aligned", uintptr(table.slotsBase), dwhtSlotBytes)
	}
	first := hint.Load()
	if first != dwhtSlotLayoutRaw && first != dwhtSlotLayoutRot8 {
		t.Fatalf("hint=%d, want raw or rot8", first)
	}
	for i := range uintptr(4) {
		slot := table.slot(i)
		if unsafe.Pointer(slot) != unsafe.Add(table.slotsBase, i*dwhtSlotBytes) {
			t.Fatalf("slot(%d) address mismatch", i)
		}
		if unsafe.Pointer(&slot.entry) != unsafe.Add(unsafe.Pointer(slot), unsafe.Sizeof(uintptr(0))) {
			t.Fatalf("slot(%d) entry word is not adjacent to ctrl word", i)
		}
	}

	table = newDWHTTable[int, int](128, uintptr(dwhtMaxProbeThreshold))
	if uintptr(table.slotsBase)&(dwhtSlotBytes-1) != 0 {
		t.Fatalf("hinted slotsBase=%#x is not %d-byte aligned", uintptr(table.slotsBase), dwhtSlotBytes)
	}
	if table.slotsRaw == nil {
		t.Fatal("hinted slot storage is nil")
	}
	if got := hint.Load(); got != first {
		t.Fatalf("hint changed from %d to %d", first, got)
	}
}

func TestDWHTMapBasic(t *testing.T) {
	m := NewDWHTMap[string, int](WithCapacity(2))

	if _, ok := m.Load("missing"); ok {
		t.Fatal("unexpected value for missing key")
	}

	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("a", 3)

	if got, ok := m.Load("a"); !ok || got != 3 {
		t.Fatalf("Load(a)=(%d,%v), want (3,true)", got, ok)
	}

	if got, loaded := m.LoadOrStore("a", 4); !loaded || got != 3 {
		t.Fatalf("LoadOrStore existing=(%d,%v), want (3,true)", got, loaded)
	}
	if got, loaded := m.LoadOrStore("c", 5); loaded || got != 5 {
		t.Fatalf("LoadOrStore new=(%d,%v), want (5,false)", got, loaded)
	}

	if prev, loaded := m.LoadAndUpdate("a", 6); !loaded || prev != 3 {
		t.Fatalf("LoadAndUpdate(a)=(%d,%v), want (3,true)", prev, loaded)
	}
	if got, ok := m.Load("a"); !ok || got != 6 {
		t.Fatalf("Load(a) after LoadAndUpdate=(%d,%v), want (6,true)", got, ok)
	}
	if prev, loaded := m.LoadAndUpdate("missing", 9); loaded || prev != 0 {
		t.Fatalf("LoadAndUpdate(missing)=(%d,%v), want (0,false)", prev, loaded)
	}
	if _, ok := m.Load("missing"); ok {
		t.Fatal("LoadAndUpdate inserted missing key")
	}

	if !m.CompareAndSwap("a", 6, 7) {
		t.Fatal("CompareAndSwap should succeed")
	}
	if m.CompareAndSwap("a", 6, 8) {
		t.Fatal("CompareAndSwap should fail with stale value")
	}
	if got, ok := m.Load("a"); !ok || got != 7 {
		t.Fatalf("Load(a) after CAS=(%d,%v), want (7,true)", got, ok)
	}

	if m.CompareAndDelete("a", 6) {
		t.Fatal("CompareAndDelete should fail with stale value")
	}
	if !m.CompareAndDelete("a", 7) {
		t.Fatal("CompareAndDelete should succeed")
	}
	if _, ok := m.Load("a"); ok {
		t.Fatal("deleted key is still present")
	}

	m.Store("d", 10)
	if prev, loaded := m.LoadAndDelete("d"); !loaded || prev != 10 {
		t.Fatalf("LoadAndDelete(d)=(%d,%v), want (10,true)", prev, loaded)
	}
	if _, loaded := m.LoadAndDelete("d"); loaded {
		t.Fatal("LoadAndDelete should return false for already deleted key")
	}

	m.Delete("b")
}

func TestDWHTMapZeroValueReady(t *testing.T) {
	var m DWHTMap[string, int]

	if got, ok := m.Load("missing"); ok || got != 0 {
		t.Fatalf("Load(missing)=(%d,%v), want (0,false)", got, ok)
	}

	m.Store("a", 1)
	if got, ok := m.Load("a"); !ok || got != 1 {
		t.Fatalf("Load(a) after Store=(%d,%v), want (1,true)", got, ok)
	}

	if got, loaded := m.LoadOrStore("a", 2); !loaded || got != 1 {
		t.Fatalf("LoadOrStore existing=(%d,%v), want (1,true)", got, loaded)
	}
	if got, loaded := m.LoadOrStore("b", 3); loaded || got != 3 {
		t.Fatalf("LoadOrStore new=(%d,%v), want (3,false)", got, loaded)
	}

	if prev, loaded := m.LoadAndUpdate("a", 4); !loaded || prev != 1 {
		t.Fatalf("LoadAndUpdate(a)=(%d,%v), want (1,true)", prev, loaded)
	}
	if prev, loaded := m.LoadAndUpdate("missing", 9); loaded || prev != 0 {
		t.Fatalf("LoadAndUpdate(missing)=(%d,%v), want (0,false)", prev, loaded)
	}

	if !m.CompareAndSwap("a", 4, 5) {
		t.Fatal("CompareAndSwap should succeed")
	}
	if m.CompareAndSwap("a", 4, 6) {
		t.Fatal("CompareAndSwap should fail with stale value")
	}

	if m.CompareAndDelete("a", 4) {
		t.Fatal("CompareAndDelete should fail with stale value")
	}
	if !m.CompareAndDelete("a", 5) {
		t.Fatal("CompareAndDelete should succeed")
	}
	if _, ok := m.Load("a"); ok {
		t.Fatal("key a should be deleted")
	}

	if prev, loaded := m.LoadAndDelete("b"); !loaded || prev != 3 {
		t.Fatalf("LoadAndDelete(b)=(%d,%v), want (3,true)", prev, loaded)
	}
	if prev, loaded := m.LoadAndDelete("b"); loaded || prev != 0 {
		t.Fatalf("LoadAndDelete(b) second=(%d,%v), want (0,false)", prev, loaded)
	}

	count := 0
	m.Range(func(k string, v int) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("Range count=%d, want 0", count)
	}
}

func TestDWHTMapGrow(t *testing.T) {
	m := NewDWHTMap[int, int](WithCapacity(1))
	for i := range 4096 {
		m.Store(i, i*10)
	}
	for i := range 4096 {
		got, ok := m.Load(i)
		if !ok || got != i*10 {
			t.Fatalf("Load(%d)=(%d,%v), want (%d,true)", i, got, ok, i*10)
		}
	}
	if got := m.Size(); got != 4096 {
		t.Fatalf("Size()=%d, want 4096", got)
	}

	count := 0
	m.Range(func(k, v int) bool {
		if v != k*10 {
			t.Errorf("Range got k=%d, v=%d, want v=%d", k, v, k*10)
		}
		count++
		return true
	})
	if count != 4096 {
		t.Fatalf("Range count=%d, want 4096", count)
	}

	// Test Range early exit
	count = 0
	for range m.All() {
		count++
		if count == 10 {
			break
		}
	}
	if count != 10 {
		t.Fatalf("Range early exit count=%d, want 10", count)
	}
}

func TestDWHTMapTombstoneCleanup(t *testing.T) {
	m := NewDWHTMap[int, int](WithCapacity(1))

	for i := range 4096 {
		m.Store(i, i)
	}
	for i := range 4096 {
		m.Delete(i)
	}
	if got := m.Size(); got != 0 {
		t.Fatalf("Size after deletes=%d, want 0", got)
	}

	for i := range 4096 {
		k := i + 4096
		m.Store(k, i)
	}
	for i := range 4096 {
		k := i + 4096
		got, ok := m.Load(k)
		if !ok || got != i {
			t.Fatalf("Load(%d)=(%d,%v), want (%d,true)", k, got, ok, i)
		}
	}
	if got := m.Size(); got != 4096 {
		t.Fatalf("Size after reinserts=%d, want 4096", got)
	}
}

func TestDWHTMapConcurrentLoadOrStoreSingleWinner(t *testing.T) {
	m := NewDWHTMap[string, int]()
	const goroutines = 64

	var stored atomic.Int32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(v int) {
			defer wg.Done()
			_, loaded := m.LoadOrStore("shared", v)
			if !loaded {
				stored.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if got := stored.Load(); got != 1 {
		t.Fatalf("stored winners=%d, want 1", got)
	}
	if _, ok := m.Load("shared"); !ok {
		t.Fatal("shared key missing")
	}
}

func TestDWHTMap_ConcurrentMixedOperations(t *testing.T) {
	m := NewDWHTMap[int, int]()
	const goroutines = 64
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			for j := range opsPerGoroutine {
				key := (id * opsPerGoroutine) + j

				// 1. Store
				m.Store(key, key*10)

				// 2. Load
				if v, ok := m.Load(key); !ok || v != key*10 {
					t.Errorf("Load(%d) failed: got (%d, %v), want (%d, true)", key, v, ok, key*10)
				}

				// 3. LoadOrStore (should return existing)
				if v, loaded := m.LoadOrStore(key, 9999); !loaded || v != key*10 {
					t.Errorf("LoadOrStore(%d) failed: got (%d, %v), want (%d, true)", key, v, loaded, key*10)
				}

				// 4. CompareAndSwap (Success)
				if !m.CompareAndSwap(key, key*10, key*20) {
					t.Errorf("CompareAndSwap(%d) failed unexpectedly", key)
				}

				// 5. CompareAndSwap (Fail - stale value)
				// We need to wait for other goroutines to potentially not interfere,
				// or ensure we use a strictly invalid value.
				// Since this is highly concurrent, the value might actually be key*10 again
				// if another goroutine just ran through step 1 (Store).
				// We use a completely invalid expected value to ensure failure.
				if m.CompareAndSwap(key, -1, key*30) {
					t.Errorf("CompareAndSwap(%d) succeeded unexpectedly with stale value", key)
				}

				// 6. Delete variants
				switch j % 3 {
				case 0:
					m.Delete(key)
					if _, ok := m.Load(key); ok {
						t.Errorf("Load(%d) succeeded after Delete", key)
					}
				case 1:
					if prev, loaded := m.LoadAndDelete(key); !loaded || prev != key*20 {
						t.Errorf("LoadAndDelete(%d) failed: got (%d, %v), want (%d, true)", key, prev, loaded, key*20)
					}
				default:
					if m.CompareAndDelete(key, -1) {
						t.Errorf("CompareAndDelete(%d) succeeded unexpectedly with stale value", key)
					}
					if !m.CompareAndDelete(key, key*20) {
						t.Errorf("CompareAndDelete(%d) failed unexpectedly", key)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	// All keys were deleted
	expectedSize := 0
	if size := m.Size(); size != expectedSize {
		t.Errorf("Final Size() = %d, want %d", size, expectedSize)
	}

	count := 0
	m.Range(func(k, v int) bool {
		count++
		return true
	})
	if count != expectedSize {
		t.Errorf("Range yielded %d items, want %d", count, expectedSize)
	}
}

func TestDWHTMapConcurrentSharedKeyChurn(t *testing.T) {
	m := NewDWHTMap[int, int](WithCapacity(1))
	const (
		goroutines = 32
		ops        = 2000
		keyMask    = 31 // keep key-space tiny to force heavy contention
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range ops {
				k := (i + id) & keyMask
				v := id*ops + i
				m.Store(k, v)
				if got, ok := m.Load(k); ok && got == v {
					_ = got
				}
				if i&1 == 0 {
					m.Delete(k)
				} else {
					_, _ = m.LoadOrStore(k, v+1)
				}
			}
		}(g)
	}
	wg.Wait()

	// Validate that map is still internally consistent and operable.
	seen := make(map[int]int)
	m.Range(func(k, v int) bool {
		seen[k] = v
		return true
	})
	if len(seen) > keyMask+1 {
		t.Fatalf("unexpected key count: got=%d want<=%d", len(seen), keyMask+1)
	}
	for k, v := range seen {
		got, ok := m.Load(k)
		if !ok || got != v {
			t.Fatalf("post-churn mismatch key=%d got=(%d,%v) want=(%d,true)", k, got, ok, v)
		}
	}
}

func TestDWHTMapClearBasic(t *testing.T) {
	m := NewDWHTMap[int, int](WithCapacity(16))
	for i := range 256 {
		m.Store(i, i+1)
	}
	if got := m.Size(); got != 256 {
		t.Fatalf("Size before Clear=%d, want 256", got)
	}

	m.Clear()
	if got := m.Size(); got != 0 {
		t.Fatalf("Size after Clear=%d, want 0", got)
	}
	for i := range 256 {
		if _, ok := m.Load(i); ok {
			t.Fatalf("Load(%d) should miss after Clear", i)
		}
	}

	// Idempotent clear on an already-empty map.
	m.Clear()
	if got := m.Size(); got != 0 {
		t.Fatalf("Size after second Clear=%d, want 0", got)
	}

	// Map should remain fully usable after clear.
	for i := range 64 {
		m.Store(i, i*10)
	}
	for i := range 64 {
		got, ok := m.Load(i)
		if !ok || got != i*10 {
			t.Fatalf("Load(%d) after Clear+Store=(%d,%v), want (%d,true)", i, got, ok, i*10)
		}
	}
}

func TestDWHTMapSameKeyTombstoneReuse(t *testing.T) {
	m := NewDWHTMap[int, int](WithCapacity(1))
	for i := range 10000 {
		actual, loaded := m.LoadOrStore(7, i)
		if loaded {
			t.Fatalf("LoadOrStore iteration %d loaded existing value %d", i, actual)
		}
		if got, ok := m.Load(7); !ok || got != i {
			t.Fatalf("Load iteration %d=(%d,%v), want (%d,true)", i, got, ok, i)
		}
		if prev, loaded := m.LoadAndDelete(7); !loaded || prev != i {
			t.Fatalf("LoadAndDelete iteration %d=(%d,%v), want (%d,true)", i, prev, loaded, i)
		}
		if got := m.Size(); got != 0 {
			t.Fatalf("Size iteration %d=%d, want 0", i, got)
		}
	}

	table := m.table.Load()
	if table == nil {
		t.Fatal("table is nil")
	}
	if slotLen := table.mask + 1; slotLen != dwhtMinSlots {
		t.Fatalf("same-key tombstone churn grew table to %d slots, want %d", slotLen, dwhtMinSlots)
	}
}

func TestDWHTMapLongProbeLimitSurvivesResize(t *testing.T) {
	m := NewDWHTMap[int, int](
		WithCapacity(1),
		WithKeyHasher(func(int, uintptr) uintptr { return 0 }),
	)
	const n = dwhtMaxProbeThreshold + 2
	for i := range n {
		m.Store(i, i*10)
	}
	for i := range n {
		got, ok := m.Load(i)
		if !ok || got != i*10 {
			t.Fatalf("Load(%d)=(%d,%v), want (%d,true)", i, got, ok, i*10)
		}
	}
}
