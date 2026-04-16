package cc

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// ============================================================================
// Basic Tests
// ============================================================================

func TestFunnelMap_StoreLoad(t *testing.T) {
	m := NewFunnelMap[int, int]()

	m.Store(1, 100)
	if v, ok := m.Load(1); !ok || v != 100 {
		t.Fatalf("Load(1) = %v, %v; want 100, true", v, ok)
	}
	if _, ok := m.Load(2); ok {
		t.Fatal("Load(2) should be missing")
	}
}

func TestFunnelMap_StoreLoadString(t *testing.T) {
	m := NewFunnelMap[string, string]()

	m.Store("foo", "bar")
	if v, ok := m.Load("foo"); !ok || v != "bar" {
		t.Fatalf("Load(foo) = %v, %v; want bar, true", v, ok)
	}
}

func TestFunnelMap_Update(t *testing.T) {
	m := NewFunnelMap[int, int]()

	m.Store(1, 1)
	m.Store(1, 2)
	if v, ok := m.Load(1); !ok || v != 2 {
		t.Fatalf("Load(1) after update = %v, %v; want 2, true", v, ok)
	}
}

func TestFunnelMap_Delete(t *testing.T) {
	m := NewFunnelMap[int, int]()

	m.Store(1, 1)
	m.Delete(1)
	if _, ok := m.Load(1); ok {
		t.Fatal("Load after Delete should be missing")
	}
	// delete non-existent key is a no-op
	m.Delete(999)
}

func TestFunnelMap_LoadOrStore(t *testing.T) {
	m := NewFunnelMap[int, int]()

	actual, loaded := m.LoadOrStore(1, 10)
	if loaded || actual != 10 {
		t.Fatalf("LoadOrStore insert: got %v, %v; want 10, false", actual, loaded)
	}
	actual, loaded = m.LoadOrStore(1, 20)
	if !loaded || actual != 10 {
		t.Fatalf("LoadOrStore existing: got %v, %v; want 10, true", actual, loaded)
	}
}

func TestFunnelMap_LoadAndDelete(t *testing.T) {
	m := NewFunnelMap[int, int]()

	m.Store(1, 42)
	prev, ok := m.LoadAndDelete(1)
	if !ok || prev != 42 {
		t.Fatalf("LoadAndDelete: got %v, %v; want 42, true", prev, ok)
	}
	if _, ok = m.Load(1); ok {
		t.Fatal("key should be absent after LoadAndDelete")
	}
	_, ok = m.LoadAndDelete(1)
	if ok {
		t.Fatal("LoadAndDelete on missing key should return false")
	}
}

func TestFunnelMap_Swap(t *testing.T) {
	m := NewFunnelMap[int, int]()

	prev, loaded := m.Swap(1, 10)
	if loaded || prev != 0 {
		t.Fatalf("Swap insert: got %v, %v; want 0, false", prev, loaded)
	}
	prev, loaded = m.Swap(1, 20)
	if !loaded || prev != 10 {
		t.Fatalf("Swap replace: got %v, %v; want 10, true", prev, loaded)
	}
	if v, ok := m.Load(1); !ok || v != 20 {
		t.Fatalf("Load after Swap: got %v, %v; want 20, true", v, ok)
	}
}

func TestFunnelMap_LoadAndUpdate(t *testing.T) {
	m := NewFunnelMap[int, int]()

	_, ok := m.LoadAndUpdate(1, 99)
	if ok {
		t.Fatal("LoadAndUpdate on missing key should return false")
	}
	m.Store(1, 1)
	prev, ok := m.LoadAndUpdate(1, 99)
	if !ok || prev != 1 {
		t.Fatalf("LoadAndUpdate: got %v, %v; want 1, true", prev, ok)
	}
	if v, _ := m.Load(1); v != 99 {
		t.Fatalf("value after LoadAndUpdate: got %v; want 99", v)
	}
}

func TestFunnelMap_Size(t *testing.T) {
	m := NewFunnelMap[int, int]()

	for i := range 100 {
		m.Store(i, i)
	}
	if s := m.Size(); s != 100 {
		t.Fatalf("Size = %d; want 100", s)
	}
	for i := range 50 {
		m.Delete(i)
	}
	if s := m.Size(); s != 50 {
		t.Fatalf("Size after deletes = %d; want 50", s)
	}
}

func TestFunnelMap_Range(t *testing.T) {
	m := NewFunnelMap[int, int]()

	const n = 50
	for i := range n {
		m.Store(i, i*2)
	}

	seen := make(map[int]int)
	m.Range(func(k, v int) bool {
		seen[k] = v
		return true
	})
	if len(seen) != n {
		t.Fatalf("Range saw %d entries; want %d", len(seen), n)
	}
	for k, v := range seen {
		if v != k*2 {
			t.Fatalf("Range: key %d has value %d; want %d", k, v, k*2)
		}
	}
}

func TestFunnelMap_RangeEarlyExit(t *testing.T) {
	m := NewFunnelMap[int, int]()

	for i := range 100 {
		m.Store(i, i)
	}
	count := 0
	m.Range(func(_, _ int) bool {
		count++
		return count < 10
	})
	if count != 10 {
		t.Fatalf("early-exit Range: saw %d; want 10", count)
	}
}

func TestFunnelMap_GrowShrink(t *testing.T) {
	m := NewFunnelMap[int, int]()

	m.Grow(1000)
	for i := range 1000 {
		m.Store(i, i)
	}
	if s := m.Size(); s != 1000 {
		t.Fatalf("Size after Grow+Store = %d; want 1000", s)
	}
	for i := range 1000 {
		m.Delete(i)
	}
	m.Shrink()
	if s := m.Size(); s != 0 {
		t.Fatalf("Size after Shrink = %d; want 0", s)
	}
}

// TestFunnelMap_BulkStoreLoad verifies correctness of storing and loading many entries
// (exercises overflow and resize paths).
func TestFunnelMap_BulkStoreLoad(t *testing.T) {
	const n = 2000
	m := NewFunnelMap[int, int]()

	for i := range n {
		m.Store(i, i*3)
	}
	for i := range n {
		v, ok := m.Load(i)
		if !ok || v != i*3 {
			t.Fatalf("Load(%d) = %v, %v; want %d, true", i, v, ok, i*3)
		}
	}
}

func TestFunnelMap_LoadOrStoreFn(t *testing.T) {
	m := NewFunnelMap[string, int]()

	// 1. Should store since key doesn't exist
	v1, loaded := m.LoadOrStoreFn("key1", func() int { return 100 })
	if loaded || v1 != 100 {
		t.Fatalf("LoadOrStoreFn: expected loaded=false, v=100, got loaded=%v, v=%v", loaded, v1)
	}

	// 2. Should load since key exists, fn should not be executed
	executed := false
	v2, loaded := m.LoadOrStoreFn("key1", func() int {
		executed = true
		return 200
	})
	if !loaded || v2 != 100 || executed {
		t.Fatalf("LoadOrStoreFn: expected loaded=true, v=100, executed=false, got loaded=%v, v=%v, executed=%v", loaded, v2, executed)
	}
}

func TestFunnelMap_CompareAndSwap(t *testing.T) {
	m := NewFunnelMap[string, int]()
	m.Store("key1", 100)

	// 1. Swap with wrong old value
	if swapped := m.CompareAndSwap("key1", 50, 200); swapped {
		t.Fatalf("CompareAndSwap: expected false when old value doesn't match")
	}
	if v, _ := m.Load("key1"); v != 100 {
		t.Fatalf("CompareAndSwap: value should remain 100, got %v", v)
	}

	// 2. Swap with correct old value
	if swapped := m.CompareAndSwap("key1", 100, 200); !swapped {
		t.Fatalf("CompareAndSwap: expected true when old value matches")
	}
	if v, _ := m.Load("key1"); v != 200 {
		t.Fatalf("CompareAndSwap: value should be updated to 200, got %v", v)
	}

	// 3. Swap non-existent key
	if swapped := m.CompareAndSwap("key2", 0, 200); swapped {
		t.Fatalf("CompareAndSwap: expected false for non-existent key")
	}
}

func TestFunnelMap_CompareAndDelete(t *testing.T) {
	m := NewFunnelMap[string, int]()
	m.Store("key1", 100)

	// 1. Delete with wrong old value
	if deleted := m.CompareAndDelete("key1", 50); deleted {
		t.Fatalf("CompareAndDelete: expected false when old value doesn't match")
	}
	if _, ok := m.Load("key1"); !ok {
		t.Fatalf("CompareAndDelete: key should still exist")
	}

	// 2. Delete with correct old value
	if deleted := m.CompareAndDelete("key1", 100); !deleted {
		t.Fatalf("CompareAndDelete: expected true when old value matches")
	}
	if _, ok := m.Load("key1"); ok {
		t.Fatalf("CompareAndDelete: key should be deleted")
	}
}

// Test FunnelMap_Compute with missing values correctly
func TestFunnelMap_Compute_Init(t *testing.T) {
	m := NewFunnelMap[string, int]()

	// 1. Compute on non-existent key
	v1, loaded := m.Compute("key1", func(e *MapEntry[string, int]) {
		if e.Loaded() {
			t.Fatalf("Compute: expected entry to not be loaded")
		}
		e.Update(10)
	})
	if loaded || v1 != 10 {
		t.Fatalf("Compute: expected loaded=false, v=10, got loaded=%v, v=%v", loaded, v1)
	}

	// 2. Verify stored value
	if v, ok := m.Load("key1"); !ok || v != 10 {
		t.Fatalf("Compute: expected key1 to be 10, got %v, %v", v, ok)
	}
}

func TestFunnelMap_AllAndEntries(t *testing.T) {
	m := NewFunnelMap[int, int]()
	for i := range 5 {
		m.Store(i, i*10)
	}

	// Test All()
	countAll := 0
	for k, v := range m.All() {
		if v != k*10 {
			t.Fatalf("All: unexpected value %v for key %v", v, k)
		}
		countAll++
	}
	if countAll != 5 {
		t.Fatalf("All: expected 5 elements, got %v", countAll)
	}

	// Test Entries()
	countEntries := 0
	for e := range m.Entries() {
		if e.Value() != e.Key()*10 {
			t.Fatalf("Entries: unexpected value %v for key %v", e.Value(), e.Key())
		}
		countEntries++
	}
	if countEntries != 5 {
		t.Fatalf("Entries: expected 5 elements, got %v", countEntries)
	}
}

func TestFunnelMap_ToMap(t *testing.T) {
	m := NewFunnelMap[int, int]()
	for i := range 10 {
		m.Store(i, i)
	}

	// 1. Without limit
	mapAll := m.ToMap()
	if len(mapAll) != 10 {
		t.Fatalf("ToMap: expected len 10, got %v", len(mapAll))
	}

	// 2. With limit
	mapLimited := m.ToMap(5)
	if len(mapLimited) != 5 {
		t.Fatalf("ToMap(limit): expected len 5, got %v", len(mapLimited))
	}
}

func TestFunnelMap_Clear(t *testing.T) {
	m := NewFunnelMap[int, int]()
	for i := range 100 {
		m.Store(i, i)
	}

	if m.Size() != 100 {
		t.Fatalf("Clear: setup failed, expected size 100, got %v", m.Size())
	}

	m.Clear()

	if m.Size() != 0 {
		t.Fatalf("Clear: expected size 0, got %v", m.Size())
	}
	// Verify practically empty
	count := 0
	m.Range(func(_, _ int) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("Clear: range yielded %v elements", count)
	}
}

func TestFunnelMap_CloneTo(t *testing.T) {
	src := NewFunnelMap[int, int]()
	for i := range 50 {
		src.Store(i, i*2)
	}

	dst := NewFunnelMap[int, int]()
	src.CloneTo(dst)

	if dst.Size() != 50 {
		t.Fatalf("CloneTo: expected dst size 50, got %v", dst.Size())
	}

	for i := range 50 {
		if v, ok := dst.Load(i); !ok || v != i*2 {
			t.Fatalf("CloneTo: expected key %v to have value %v, got %v, ok=%v", i, i*2, v, ok)
		}
	}
}

func TestFunnelMap_Compute_UpdateAndDelete(t *testing.T) {
	m := NewFunnelMap[string, int]()
	m.Store("key1", 10)

	// 1. Compute on existing key
	v2, loaded := m.Compute("key1", func(e *MapEntry[string, int]) {
		if !e.Loaded() || e.Value() != 10 {
			t.Fatalf("Compute: expected entry to be loaded with value 10")
		}
		e.Update(20)
	})
	if !loaded || v2 != 20 {
		t.Fatalf("Compute: expected loaded=true, v=20, got loaded=%v, v=%v", loaded, v2)
	}

	// 2. Verify stored value
	if v, ok := m.Load("key1"); !ok || v != 20 {
		t.Fatalf("Compute: expected key1 to be 20, got %v, %v", v, ok)
	}

	// 3. Compute to delete
	_, loaded = m.Compute("key1", func(e *MapEntry[string, int]) {
		e.Delete()
	})
	if !loaded {
		t.Fatalf("Compute: expected loaded=true before deletion")
	}

	// 4. Verify deleted value
	if _, ok := m.Load("key1"); ok {
		t.Fatalf("Compute: expected key to be deleted")
	}
}

func TestFunnelMap_ComputeRange(t *testing.T) {
	m := NewFunnelMap[int, string]()
	for i := range 10 {
		m.Store(i, strconv.Itoa(i))
	}

	// Double values for evens, delete odds
	m.ComputeRange(func(e *MapEntry[int, string]) bool {
		if e.Key()%2 == 0 {
			e.Update(e.Value() + e.Value())
		} else {
			e.Delete()
		}
		return true
	})

	if m.Size() != 5 {
		t.Fatalf("ComputeRange: expected size 5 after deletions, got %v", m.Size())
	}

	for i := range 10 {
		v, ok := m.Load(i)
		if i%2 == 0 {
			if !ok || v != strconv.Itoa(i)+strconv.Itoa(i) {
				t.Fatalf("ComputeRange: expected updated value for key %v", i)
			}
		} else {
			if ok {
				t.Fatalf("ComputeRange: expected key %v to be deleted", i)
			}
		}
	}
}

// ============================================================================
// Concurrent Tests
// ============================================================================

// TestFunnelMap_ConcurrentStoreLoad runs parallel stores on disjoint keys, then
// verifies all entries via Load (avoids relying on potentially-approximate Size).
func TestFunnelMap_ConcurrentStoreLoad(t *testing.T) {
	const goroutines = 16
	const perG = 500

	m := NewFunnelMap[int, int]()
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range perG {
				key := base*perG + i
				m.Store(key, key)
			}
		}(g)
	}
	wg.Wait()

	// Verify all entries via Load (more reliable than Size under concurrent resize)
	for g := range goroutines {
		for i := range perG {
			key := g*perG + i
			if v, ok := m.Load(key); !ok || v != key {
				t.Fatalf("Load(%d) = %v, %v; want %d, true", key, v, ok, key)
			}
		}
	}
}

// TestFunnelMap_ConcurrentReadWrite runs concurrent readers and writers on shared keys.
func TestFunnelMap_ConcurrentReadWrite(t *testing.T) {
	const (
		keys      = 64
		writers   = 4
		readers   = 8
		itersPerW = 2000
	)

	m := NewFunnelMap[int, int64]()
	for i := range keys {
		m.Store(i, 0)
	}

	var writerWg, readerWg sync.WaitGroup
	stop := make(chan struct{})

	for w := range writers {
		writerWg.Add(1)
		go func(id int) {
			defer writerWg.Done()
			for i := range itersPerW {
				key := (id*itersPerW + i) % keys
				m.Swap(key, int64(id*itersPerW+i))
			}
		}(w)
	}

	for range readers {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := range keys {
						m.Load(i)
					}
				}
			}
		}()
	}

	writerWg.Wait()
	close(stop)
	readerWg.Wait()
}

// TestFunnelMap_ConcurrentLoadOrStore exercises LoadOrStore races;
// exactly one goroutine should win the insert.
func TestFunnelMap_ConcurrentLoadOrStore(t *testing.T) {
	const goroutines = 32
	const key = 1

	m := NewFunnelMap[int, int]()
	var wins atomic.Int64
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, loaded := m.LoadOrStore(key, id)
			if !loaded {
				wins.Add(1)
			}
		}(g)
	}
	wg.Wait()

	if w := wins.Load(); w != 1 {
		t.Fatalf("expected exactly 1 winner for LoadOrStore, got %d", w)
	}
	if _, ok := m.Load(key); !ok {
		t.Fatal("key should exist after LoadOrStore")
	}
}

// TestFunnelMap_ConcurrentDelete exercises concurrent deletes; verifies entries are absent.
func TestFunnelMap_ConcurrentDelete(t *testing.T) {
	const n = 500

	m := NewFunnelMap[int, int]()
	for i := range n {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(key int) {
			defer wg.Done()
			m.Delete(key)
		}(i)
	}
	wg.Wait()

	// verify all keys are gone
	for i := range n {
		if _, ok := m.Load(i); ok {
			t.Fatalf("key %d should be absent after concurrent delete", i)
		}
	}
}

// TestFunnelMap_ConcurrentGrow triggers concurrent resizes under load and validates
// all entries are accessible.
func TestFunnelMap_ConcurrentGrow(t *testing.T) {
	const goroutines = 8
	const perG = 200

	m := NewFunnelMap[string, int]()
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range perG {
				key := fmt.Sprintf("k%d", base*perG+i)
				m.Store(key, base*perG+i)
			}
		}(g)
	}
	wg.Wait()

	// verify via Load, not Size
	for g := range goroutines {
		for i := range perG {
			key := fmt.Sprintf("k%d", g*perG+i)
			want := g*perG + i
			if v, ok := m.Load(key); !ok || v != want {
				t.Fatalf("Load(%s) = %v, %v; want %d, true", key, v, ok, want)
			}
		}
	}
}

// TestFunnelMap_ConcurrentRange validates Range doesn't panic under concurrent writes.
func TestFunnelMap_ConcurrentRange(t *testing.T) {
	m := NewFunnelMap[int, int]()
	for i := range 200 {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				m.Store(i%200, i)
			}
		}
	}()

	for range 10 {
		m.Range(func(_, _ int) bool { return true })
	}
	close(stop)
	wg.Wait()
}
