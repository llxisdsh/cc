package cc

import (
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
	if !m.Delete(1) {
		t.Fatalf("expected delete to return true")
	}
	if _, ok := m.Load(1); ok {
		t.Fatalf("expected 1 to be deleted")
	}
	if m.Delete(1) {
		t.Fatalf("expected delete of missing key to return false")
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
	if deleted := m.Delete(100); deleted {
		t.Fatalf("expected false when deleting non-existent key")
	}
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

	var deleteCount int32

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m.Delete(key) {
				atomic.AddInt32(&deleteCount, 1)
			}
		}()
	}
	wg.Wait()

	if deleteCount != 1 {
		t.Fatalf("expected exactly 1 successful delete, got %d", deleteCount)
	}
	if m.Size() != 0 {
		t.Fatalf("expected size 0, got %d", m.Size())
	}
}
