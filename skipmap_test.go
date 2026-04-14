package cc

import (
	"sync"
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
			for j := 0; j < items; j++ {
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
			for j := 0; j < items; j++ {
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
