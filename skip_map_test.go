package cc

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSkipMap_Basic(t *testing.T) {
	m := NewSkipMap[int, string]()

	// Test Store & Load
	m.Store(1, "a")
	m.Store(2, "b")

	if v, ok := m.Load(1); !ok || v != "a" {
		t.Fatalf("expected 1: a, got %v: %v", ok, v)
	}

	if v, ok := m.Load(3); ok {
		t.Fatalf("expected not found, got %v", v)
	}

	// Test Delete
	m.Delete(1)
	if _, ok := m.Load(1); ok {
		t.Fatalf("expected 1 to be deleted")
	}
	if _, ok := m.LoadAndDelete(1); ok {
		t.Fatalf("expected LoadAndDelete of missing key to return false")
	}

	// Test LoadAndDelete
	m.Store(4, "d")
	if v, ok := m.LoadAndDelete(4); !ok || v != "d" {
		t.Fatalf("expected 4: d, got %v: %v", ok, v)
	}
	if _, ok := m.Load(4); ok {
		t.Fatalf("expected 4 to be deleted")
	}

	// Test LoadOrStore
	if v, loaded := m.LoadOrStore(5, "e"); loaded || v != "e" {
		t.Fatalf("expected to store 5: e, got %v: %v", loaded, v)
	}
	if v, loaded := m.LoadOrStore(5, "ee"); !loaded || v != "e" {
		t.Fatalf("expected to load 5: e, got %v: %v", loaded, v)
	}
}

func TestSkipMap_ZeroValue(t *testing.T) {
	var m SkipMap[int, string]

	if m.Size() != 0 {
		t.Fatalf("expected zero-value map size 0, got %d", m.Size())
	}
	if v, ok := m.Load(1); ok || v != "" {
		t.Fatalf("expected missing zero value, got %q: %v", v, ok)
	}
	m.Delete(1)
	m.Range(func(key int, value string) bool {
		t.Fatalf("empty zero-value map should not iterate")
		return true
	})
	if m.head != nil {
		t.Fatalf("read-only zero-value operations should not initialize head")
	}

	m.Store(1, "a")
	if v, ok := m.Load(1); !ok || v != "a" {
		t.Fatalf("expected 1: a, got %q: %v", v, ok)
	}

	if v, loaded := m.LoadOrStore(1, "b"); !loaded || v != "a" {
		t.Fatalf("expected existing 1: a, got %q: %v", v, loaded)
	}
	if v, loaded := m.LoadOrStore(2, "b"); loaded || v != "b" {
		t.Fatalf("expected stored 2: b, got %q: %v", v, loaded)
	}

	seen := 0
	m.Range(func(key int, value string) bool {
		seen++
		return true
	})
	if seen != 2 {
		t.Fatalf("expected 2 range entries, got %d", seen)
	}

	if v, ok := m.LoadAndDelete(1); !ok || v != "a" {
		t.Fatalf("expected deleted 1: a, got %q: %v", v, ok)
	}
	if m.Size() != 1 {
		t.Fatalf("expected size 1 after delete, got %d", m.Size())
	}
}

func TestSkipMap_ZeroValueConcurrentFirstUse(t *testing.T) {
	var m SkipMap[int, int]
	var wg sync.WaitGroup
	const workers = 100

	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Store(i, i*i)
		}(i)
	}
	wg.Wait()

	if m.Size() != workers {
		t.Fatalf("expected %d elements, got %d", workers, m.Size())
	}
	for i := range workers {
		if v, ok := m.Load(i); !ok || v != i*i {
			t.Fatalf("expected %d: %d, got %d: %v", i, i*i, v, ok)
		}
	}
}

func TestSkipMap_Concurrency(t *testing.T) {
	m := NewSkipMap[int, int]()
	var wg sync.WaitGroup
	const workers = 100
	const items = 1000

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range items {
				m.Store(id*items+j, j)
			}
		}(i)
	}
	wg.Wait()

	if m.Size() != workers*items {
		t.Fatalf("expected %d elements, got %d", workers*items, m.Size())
	}

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range items {
				m.Delete(id*items + j)
			}
		}(i)
	}
	wg.Wait()

	if m.Size() != 0 {
		t.Fatalf("expected 0 elements after deletion, got %d", m.Size())
	}
}

func TestSkipMap_Range(t *testing.T) {
	m := NewSkipMap[int, int]()
	sum := 0
	for i := 1; i <= 10; i++ {
		m.Store(i, i*10)
		sum += i * 10
	}

	actualSum := 0
	count := 0
	m.Range(func(key int, value int) bool {
		actualSum += value
		count++
		return true
	})

	if count != 10 {
		t.Fatalf("expected 10 elements, got %d", count)
	}
	if actualSum != sum {
		t.Fatalf("expected sum %d, got %d", sum, actualSum)
	}
}

func TestSkipMap_LoadOrStoreFn(t *testing.T) {
	m := NewSkipMap[int, string]()
	called := 0
	f := func() string {
		called++
		return "lazy"
	}

	// Should call
	if v, loaded := m.LoadOrStoreFn(1, f); loaded || v != "lazy" {
		t.Fatalf("expected to store lazy, got %v: %v", loaded, v)
	}
	if called != 1 {
		t.Fatalf("expected called 1, got %d", called)
	}

	// Should NOT call
	if v, loaded := m.LoadOrStoreFn(1, f); !loaded || v != "lazy" {
		t.Fatalf("expected to load lazy, got %v: %v", loaded, v)
	}
	if called != 1 {
		t.Fatalf("expected called 1 (no increase), got %d", called)
	}
}

func TestSkipMap_AllIterator(t *testing.T) {
	m := NewSkipMap[int, int]()

	// Test empty map
	for range m.All() {
		t.Fatalf("empty map should not iterate")
	}

	// Store items
	sum := 0
	for i := 1; i <= 10; i++ {
		m.Store(i, i*10)
		sum += i * 10
	}

	// Test All iterator
	actualSum := 0
	count := 0
	for k, v := range m.All() {
		if k*10 != v {
			t.Fatalf("expected value %d for key %d, got %d", k*10, k, v)
		}
		actualSum += v
		count++
	}

	if count != 10 {
		t.Fatalf("expected 10 elements, got %d", count)
	}
	if actualSum != sum {
		t.Fatalf("expected sum %d, got %d", sum, actualSum)
	}

	// Test Early Exit
	count = 0
	for range m.All() {
		count++
		if count == 5 {
			break
		}
	}
	if count != 5 {
		t.Fatalf("expected early exit after 5 elements, got %d", count)
	}
}

func TestSkipMap_UpdateExisting(t *testing.T) {
	m := NewSkipMap[int, string]()
	m.Store(1, "a")
	if v, ok := m.Load(1); !ok || v != "a" {
		t.Fatalf("expected a, got %v", v)
	}

	// Update existing
	m.Store(1, "b")
	if v, ok := m.Load(1); !ok || v != "b" {
		t.Fatalf("expected b, got %v", v)
	}
}

func TestSkipMap_DeleteNonExistent(t *testing.T) {
	m := NewSkipMap[int, string]()
	m.Delete(100)
	if _, loaded := m.LoadAndDelete(100); loaded {
		t.Fatalf("expected false when LoadAndDelete non-existent key")
	}
}

func TestSkipMap_ConcurrentLoadOrStore(t *testing.T) {
	m := NewSkipMap[int, int]()
	var wg sync.WaitGroup
	const workers = 100
	const key = 42

	// We'll track how many times 'loaded' is false (meaning it was successfully stored)
	// In a concurrent environment, it should be false exactly once.
	var storeCount int32

	for i := range workers {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			_, loaded := m.LoadOrStore(key, val)
			if !loaded {
				atomic.AddInt32(&storeCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if storeCount != 1 {
		t.Fatalf("expected exactly 1 successful store, got %d", storeCount)
	}
	if m.Size() != 1 {
		t.Fatalf("expected size 1, got %d", m.Size())
	}
}

func TestSkipMap_ConcurrentLoadOrStoreFn(t *testing.T) {
	m := NewSkipMap[int, int]()
	var wg sync.WaitGroup
	const workers = 100
	const key = 99

	var executeCount int32
	var storeCount int32

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, loaded := m.LoadOrStoreFn(key, func() int {
				atomic.AddInt32(&executeCount, 1)
				return 1000
			})
			if !loaded {
				atomic.AddInt32(&storeCount, 1)
			}
		}()
	}
	wg.Wait()

	if storeCount != 1 {
		t.Fatalf("expected exactly 1 successful store, got %d", storeCount)
	}
	// Note: in a lock-free optimistic skip list, multiple goroutines might race
	// to compute the value before acquiring the lock. We don't guarantee executeCount == 1,
	// but we DO guarantee it successfully stores only once.
	if m.Size() != 1 {
		t.Fatalf("expected size 1, got %d", m.Size())
	}
}

func TestSkipMap_ConcurrentDelete(t *testing.T) {
	m := NewSkipMap[int, int]()
	var wg sync.WaitGroup
	const workers = 100
	const key = 77

	m.Store(key, 100)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Delete(key)
		}()
	}
	wg.Wait()

	if m.Size() != 0 {
		t.Fatalf("expected size 0, got %d", m.Size())
	}
}

// TestSkipMap_ConcurrentStoreStress tests SkipMap's concurrent Store
func TestSkipMap_ConcurrentStoreStress(t *testing.T) {
	for _, total := range []int{1024, 4096, 16384, 65536} {
		t.Run(strconv.Itoa(total), func(t *testing.T) {
			for attempt := 0; attempt < 100; attempt++ {
				m := NewSkipMap[string, int]()
				workers := runtime.GOMAXPROCS(0)
				keys := make([]string, total)
				for i := range total {
					keys[i] = strconv.Itoa(i)
				}

				var wg sync.WaitGroup
				wg.Add(workers)
				batch := (total + workers - 1) / workers
				for w := range workers {
					go func(id int) {
						defer wg.Done()
						s := id * batch
						e := min((id+1)*batch, total)
						for i := s; i < e; i++ {
							m.Store(keys[i], i)
						}
					}(w)
				}
				wg.Wait()

				if got := m.Size(); got != total {
					// Count via Range
					rangeCount := 0
					m.Range(func(k string, v int) bool {
						rangeCount++
						return true
					})

					missing := make([]int, 0)
					for i := range total {
						if _, ok := m.Load(keys[i]); !ok {
							missing = append(missing, i)
						}
					}
					t.Fatalf("attempt %d: SkipMap want=%d got_size=%d got_range=%d missing=%d first=%v",
						attempt, total, got, rangeCount, len(missing), missing[:min(len(missing), 10)])
				}
			}
		})
	}
}
