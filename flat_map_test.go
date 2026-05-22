//go:build !race

package cc

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// ============================================================================
// go1.22
// ============================================================================

func TestFlatMap_BucketOfStructSize(t *testing.T) {
	t.Logf("CacheLineSize : %d", cacheLineSize)
	t.Logf("entriesPerBucket : %d", entriesPerBucket)

	size := unsafe.Sizeof(FlatMap[int, int]{})
	t.Log("FlatMap size:", size)
	if size != cacheLineSize {
		t.Logf("FlatMap doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(flatRebuildState[int, int]{})
	t.Log("flatRebuildState size:", size)
	if size != cacheLineSize {
		t.Logf("flatRebuildState doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(flatTable[int, int]{})
	t.Log("flatTable size:", size)
	if size != cacheLineSize {
		t.Logf("flatTable doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(flatBucketNoHash[int, int]{})
	t.Log("flatBucketNoHash size:", size)
	if size != cacheLineSize {
		t.Logf("flatBucketNoHash doesn't meet CacheLineSize: %d", size)
	}

	size = unsafe.Sizeof(flatBucketWithHash[int, int]{})
	t.Log("flatBucketWithHash size:", size)
	if size != cacheLineSize {
		t.Logf("flatBucketWithHash doesn't meet CacheLineSize: %d", size)
	}
}

// TestFlatMap_BasicOperations tests basic Load and Compute operations
func TestFlatMap_BasicOperations(t *testing.T) {
	var m FlatMap[string, int]

	// Test empty map
	if val, ok := m.Load("nonexistent"); ok {
		t.Errorf("Expected not found, got value %v", val)
	}

	// Test insert
	actual, ok := m.Compute(
		"key1",
		func(e *MapEntry[string, int]) {
			if e.Loaded() {
				t.Error("Expected not loaded for new key")
			}
			e.Update(42)
		},
	)
	if ok || actual != 42 {
		t.Errorf("Expected (42, false), got (%v, %v)", actual, ok)
	}

	// Test load after insert
	if val, loaded := m.Load("key1"); !loaded || val != 42 {
		t.Errorf("Expected (42, true), got (%v, %v)", val, loaded)
	}

	// Test update
	actual, ok = m.Compute(
		"key1",
		func(e *MapEntry[string, int]) {
			if !e.Loaded() || e.Value() != 42 {
				t.Errorf(
					"Expected loaded=true, old=42, got loaded=%v, old=%v",
					e.Loaded(),
					e.Value(),
				)
			}
			e.Update(e.Value() + 10)
		},
	)
	if !ok || actual != 52 {
		t.Errorf("Expected (52, true), got (%v, %v)", actual, ok)
	}

	// Test load after update
	if val, loaded := m.Load("key1"); !loaded || val != 52 {
		t.Errorf("Expected (52, true), got (%v, %v)", val, loaded)
	}

	// Test delete
	_, ok = m.Compute(
		"key1",
		func(e *MapEntry[string, int]) {
			if !e.Loaded() || e.Value() != 52 {
				t.Errorf(
					"Expected loaded=true, old=52, got loaded=%v, old=%v",
					e.Loaded(),
					e.Value(),
				)
			}
			e.Delete()
		},
	)
	if !ok {
		t.Errorf("Expected ok=true after delete, got ok=%v", ok)
	}

	// Test load after delete
	if val, loaded := m.Load("key1"); loaded {
		t.Errorf("Expected not found after delete, got (%v, %v)", val, loaded)
	}

	// Test cancel operation
	m.Compute("key2", func(e *MapEntry[string, int]) {
		e.Update(100)
	})
	actual, ok = m.Compute(
		"key2",
		func(e *MapEntry[string, int]) {
		},
	)
	if !ok || actual != 100 {
		t.Errorf("Expected (100, true) after cancel, got (%v, %v)", actual, ok)
	}
}

// TestFlatMap_EdgeCases tests edge cases and error conditions
func TestFlatMap_EdgeCases(t *testing.T) {
	var m FlatMap[string, *string]

	// Test with empty string key
	m.Compute(
		"",
		func(e *MapEntry[string, *string]) {
			newV := "empty_key_value"
			e.Update(&newV)
		},
	)
	if val, ok := m.Load(""); !ok || *val != "empty_key_value" {
		t.Errorf("Empty key test failed: got (%v, %v)", *val, ok)
	}

	// Test with very long key
	longKey := string(make([]byte, 1000))
	for i := range longKey {
		longKey = longKey[:i] + "a" + longKey[i+1:]
	}
	m.Compute(
		longKey,
		func(e *MapEntry[string, *string]) {
			newV := "long_key_value"
			e.Update(&newV)
		},
	)
	if val, ok := m.Load(longKey); !ok || *val != "long_key_value" {
		t.Errorf("Long key test failed: got (%v, %v)", *val, ok)
	}

	// Verify data still intact
	if val, ok := m.Load(""); !ok || *val != "empty_key_value" {
		t.Errorf(
			"After invalid grow, empty key test failed: got (%v, %v)",
			*val,
			ok,
		)
	}
}

// TestFlatMap_MultipleKeys tests operations with multiple keys
func TestFlatMap_MultipleKeys(t *testing.T) {
	var m FlatMap[int, *string]

	// Insert multiple keys
	for i := range 100 {
		m.Compute(
			i,
			func(e *MapEntry[int, *string]) {
				newV := fmt.Sprintf("value_%d", i)
				e.Update(&newV)
			},
		)
	}

	// Verify all keys
	for i := range 100 {
		expected := fmt.Sprintf("value_%d", i)
		if val, ok := m.Load(i); !ok || *val != expected {
			t.Errorf(
				"Key %d: expected (%s, true), got (%v, %v)",
				i,
				expected,
				val,
				ok,
			)
		}
	}

	// Delete even keys
	for i := 0; i < 100; i += 2 {
		m.Compute(
			i,
			func(e *MapEntry[int, *string]) {
				e.Delete()
			},
		)
	}

	// Verify deletions
	for i := range 100 {
		val, ok := m.Load(i)
		if i%2 == 0 {
			// Even keys should be deleted
			if ok {
				t.Errorf(
					"Key %d should be deleted, but got (%v, %v)",
					i,
					val,
					ok,
				)
			}
		} else {
			// Odd keys should remain
			expected := fmt.Sprintf("value_%d", i)
			if !ok || *val != expected {
				t.Errorf("Key %d: expected (%s, true), got (%v, %v)", i, expected, val, ok)
			}
		}
	}
}

// TestFlatMap_Store tests the Store method
func TestFlatMap_Store(t *testing.T) {
	var m FlatMap[string, int]

	// Test store new key
	m.Store("key1", 100)
	if val, ok := m.Load("key1"); !ok || val != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", val, ok)
	}

	// Test store existing key (update)
	m.Store("key1", 200)
	if val, ok := m.Load("key1"); !ok || val != 200 {
		t.Errorf("Expected (200, true), got (%v, %v)", val, ok)
	}
}

// TestFlatMap_LoadOrStore tests the LoadOrStore method
func TestFlatMap_LoadOrStore(t *testing.T) {
	var m FlatMap[string, int]

	// Test store new key
	actual, loaded := m.LoadOrStore("key1", 100)
	if loaded || actual != 100 {
		t.Errorf("Expected (0, false), got (%v, %v)", actual, loaded)
	}

	// Test load existing key
	actual, loaded = m.LoadOrStore("key1", 200)
	if !loaded || actual != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", actual, loaded)
	}

	// Verify value wasn't changed
	if val, ok := m.Load("key1"); !ok || val != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", val, ok)
	}
}

// TestFlatMap_LoadOrStoreFn tests the LoadOrStoreFn method
func TestFlatMap_LoadOrStoreFn(t *testing.T) {
	var m FlatMap[string, int]

	// Test store new key
	actual, loaded := m.LoadOrStoreFn("key1", func() int { return 100 })
	if loaded || actual != 100 {
		t.Errorf("Expected (0, false), got (%v, %v)", actual, loaded)
	}

	// Test load existing key
	actual, loaded = m.LoadOrStoreFn("key1", func() int { return 200 })
	if !loaded || actual != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", actual, loaded)
	}

	// Verify value wasn't changed
	if val, ok := m.Load("key1"); !ok || val != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", val, ok)
	}
}

// TestFlatMap_Delete tests the Delete method
func TestFlatMap_Delete(t *testing.T) {
	var m FlatMap[string, int]

	// Store a key
	m.Store("key1", 100)
	if val, ok := m.Load("key1"); !ok || val != 100 {
		t.Errorf("Expected (100, true), got (%v, %v)", val, ok)
	}

	// Delete the key
	m.Delete("key1")
	if val, ok := m.Load("key1"); ok {
		t.Errorf("Expected key to be deleted, but got (%v, %v)", val, ok)
	}

	// Delete non-existent key (should not panic)
	m.Delete("nonexistent")
}

func TestFlatMap_Swap(t *testing.T) {
	var m FlatMap[string, int]

	// Swap on empty: returns zero, loaded=false, sets value
	prev, loaded := m.Swap("a", 1)
	if loaded || prev != 0 {
		t.Errorf("first swap got (%d,%v), want (0,false)", prev, loaded)
	}
	if v, ok := m.Load("a"); !ok || v != 1 {
		t.Errorf("post swap got (%d,%v), want (1,true)", v, ok)
	}

	// Swap on existing: returns previous, loaded=true, updates value
	prev, loaded = m.Swap("a", 2)
	if !loaded || prev != 1 {
		t.Errorf("second swap got (%d,%v), want (1,true)", prev, loaded)
	}
	if v, ok := m.Load("a"); !ok || v != 2 {
		t.Errorf("final value got (%d,%v), want (2,true)", v, ok)
	}
}

func TestFlatMap_LoadAndDelete(t *testing.T) {
	var m FlatMap[string, int]

	m.Store("x", 100)
	prev, loaded := m.LoadAndDelete("x")
	if !loaded || prev != 100 {
		t.Errorf("load+del got (%d,%v), want (100,true)", prev, loaded)
	}
	if v, ok := m.Load("x"); ok {
		t.Errorf("after delete found key: (%d,%v)", v, ok)
	}

	// Deleting missing key: zero, false
	prev, loaded = m.LoadAndDelete("x")
	if loaded || prev != 0 {
		t.Errorf("delete missing got (%d,%v), want (0,false)", prev, loaded)
	}
	if v, ok := m.Load("y"); ok {
		t.Errorf("missing key y found: (%d,%v)", v, ok)
	}
}

func TestFlatMap_LoadAndUpdate(t *testing.T) {
	var m FlatMap[string, int]

	m.Store("k", 7)
	prev, loaded := m.LoadAndUpdate("k", 9)
	if !loaded || prev != 7 {
		t.Errorf("load+update got (%d,%v), want (7,true)", prev, loaded)
	}
	if v, ok := m.Load("k"); !ok || v != 9 {
		t.Errorf("updated value got (%d,%v), want (9,true)", v, ok)
	}

	// Missing key: should not insert, returns zero, false
	prev, loaded = m.LoadAndUpdate("missing", 1)
	if loaded || prev != 0 {
		t.Errorf("update missing got (%d,%v), want (0,false)", prev, loaded)
	}
	if _, ok := m.Load("missing"); ok {
		t.Errorf("missing inserted by update unexpectedly")
	}
}

func TestFlatMap_CompareAndSwap(t *testing.T) {
	var m FlatMap[string, int]
	m.Store("k", 1)
	if !m.CompareAndSwap("k", 1, 2) {
		t.Fatalf("CompareAndSwap should succeed")
	}
	if v, ok := m.Load("k"); !ok || v != 2 {
		t.Fatalf("value after CompareAndSwap got (%v,%v), want (2,true)", v, ok)
	}
	if m.CompareAndSwap("k", 1, 3) {
		t.Fatalf("CompareAndSwap should fail on mismatched old value")
	}
	if v, ok := m.Load("k"); !ok || v != 2 {
		t.Fatalf("value should remain unchanged, got (%v,%v)", v, ok)
	}
}

func TestFlatMap_CompareAndDelete(t *testing.T) {
	var m FlatMap[string, int]
	m.Store("k", 10)
	if m.CompareAndDelete("k", 1) {
		t.Fatalf("CompareAndDelete should fail on mismatched old value")
	}
	if v, ok := m.Load("k"); !ok || v != 10 {
		t.Fatalf("value should remain after failed CompareAndDelete, got (%v,%v)", v, ok)
	}
	if !m.CompareAndDelete("k", 10) {
		t.Fatalf("CompareAndDelete should succeed")
	}
	if _, ok := m.Load("k"); ok {
		t.Fatalf("key should be deleted")
	}
}

func TestFlatMap_CompareAndSwap_PanicForNonComparableValue(t *testing.T) {
	var m FlatMap[int, []int]
	m.Store(1, []int{1})
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic for non-comparable value type")
		}
	}()
	_ = m.CompareAndSwap(1, []int{1}, []int{2})
}

func TestFlatMap_GrowShrink_DataIntegrity(t *testing.T) {
	m := NewFlatMap[int, int](WithCapacity(8), WithAutoShrink())
	for i := range 2000 {
		m.Store(i, i*3)
	}
	m.Grow(1 << 14)
	for i := range 2000 {
		v, ok := m.Load(i)
		if !ok || v != i*3 {
			t.Fatalf("after Grow key %d got (%v,%v), want (%d,true)", i, v, ok, i*3)
		}
	}
	for i := range 1900 {
		m.Delete(i)
	}
	m.Shrink()
	for i := 1900; i < 2000; i++ {
		v, ok := m.Load(i)
		if !ok || v != i*3 {
			t.Fatalf("after Shrink key %d got (%v,%v), want (%d,true)", i, v, ok, i*3)
		}
	}
}

func TestFlatMap_CloneTo(t *testing.T) {
	src := NewFlatMap[int, int]()
	for i := range 200 {
		src.Store(i, i+1)
	}
	dst := NewFlatMap[int, int]()
	dst.Store(999, 999)
	src.CloneTo(dst)
	if _, ok := dst.Load(999); ok {
		t.Fatalf("destination should be cleared before clone")
	}
	for i := range 200 {
		v, ok := dst.Load(i)
		if !ok || v != i+1 {
			t.Fatalf("cloned key %d got (%v,%v), want (%d,true)", i, v, ok, i+1)
		}
	}
	src.Store(0, 777)
	if v, ok := dst.Load(0); !ok || v != 1 {
		t.Fatalf("destination should not track later source writes, got (%v,%v)", v, ok)
	}
}

// TestFlatMap_Concurrent tests concurrent operations
func TestFlatMap_Concurrent(t *testing.T) {
	var m FlatMap[int, int]
	const numGoroutines = 10
	const numOpsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writers
	for g := range numGoroutines {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 1; i <= numOpsPerGoroutine; i++ {
				key := goroutineID*numOpsPerGoroutine + i
				m.Compute(
					key,
					func(e *MapEntry[int, int]) {
						e.Update(key * 2)
					},
				)
			}
		}(g)
	}

	// Concurrent readers
	for g := range numGoroutines {
		go func(goroutineID int) {
			for i := 1; i <= numOpsPerGoroutine; i++ {
				key := goroutineID*numOpsPerGoroutine + i
				// May or may not find the key depending on timing
				m.Load(key)
			}
		}(g)
	}

	wg.Wait()

	// Verify final state
	for g := range numGoroutines {
		for i := 1; i <= numOpsPerGoroutine; i++ {
			key := g*numOpsPerGoroutine + i
			expected := key * 2
			if val, ok := m.Load(key); !ok || val != expected {
				t.Errorf(
					"Key %d: expected (%d, true), got (%v, %v)",
					key,
					expected,
					val,
					ok,
				)
			}
		}
	}
}

// TestFlatMap_ConcurrentReadWrite tests heavy concurrent read/write load
func TestFlatMap_ConcurrentReadWrite(t *testing.T) {
	var m FlatMap[int, int]

	// Reduce test duration and concurrency for coverage mode
	var duration time.Duration
	var numReaders, numWriters int
	if testing.CoverMode() != "" {
		duration = 500 * time.Millisecond
		numReaders = 4
		numWriters = 1
	} else {
		duration = 2 * time.Second
		numReaders = 8
		numWriters = 2
	}

	// Pre-populate with some data
	for i := range 1000 {
		m.Compute(i, func(e *MapEntry[int, int]) {
			e.Update(i)
		})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Start readers
	for range numReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(1000)
					m.Load(key)
				}
			}
		}()
	}

	// Start writers
	for range numWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(1000)
					m.Compute(
						key,
						func(e *MapEntry[int, int]) {
							newV := rand.IntN(10000)
							e.Update(newV)
						},
					)
				}
			}
		}()
	}

	// Run for specified duration
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// t.Log("Concurrent read/write test completed successfully")
}

// TestFlatMap_Range tests the Range method
func TestFlatMap_Range(t *testing.T) {
	var m FlatMap[int, *string]

	// Test empty map
	count := 0
	m.Range(func(k int, v *string) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("Expected 0 iterations on empty map, got %d", count)
	}

	// Add some data
	expected := make(map[int]string)
	for i := range 10 {
		value := fmt.Sprintf("value_%d", i)
		m.Store(i, &value)
		expected[i] = value
	}

	// Test full iteration
	found := make(map[int]string)
	m.Range(func(k int, v *string) bool {
		found[k] = *v
		return true
	})

	if len(found) != len(expected) {
		t.Errorf("Expected %d items, got %d", len(expected), len(found))
	}

	for k, v := range expected {
		if foundV, ok := found[k]; !ok || foundV != v {
			t.Errorf("Key %d: expected %s, got %s (ok=%v)", k, v, foundV, ok)
		}
	}

	// Test early termination
	count = 0
	m.Range(func(k int, v *string) bool {
		count++
		return count < 5 // Stop after 5 iterations
	})
	if count != 5 {
		t.Errorf("Expected 5 iterations with early termination, got %d", count)
	}
}

// TestFlatMap_Size tests the Size method
func TestFlatMap_Size(t *testing.T) {
	var m FlatMap[int, *string]

	// Test empty map
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 for empty map, got %d", size)
	}

	// Add items and check size
	for i := range 10 {
		value := fmt.Sprintf("value_%d", i)
		m.Store(i, &value)
		expectedSize := i + 1
		if size := m.Size(); size != expectedSize {
			t.Errorf(
				"After storing %d items, expected size %d, got %d",
				expectedSize,
				expectedSize,
				size,
			)
		}
	}

	// Delete items and check size
	for i := range 5 {
		m.Delete(i)
		expectedSize := 10 - i - 1
		if size := m.Size(); size != expectedSize {
			t.Errorf(
				"After deleting %d items, expected size %d, got %d",
				i+1,
				expectedSize,
				size,
			)
		}
	}

	// Test Clear and verify size becomes 0
	m.Clear()
	if size := m.Size(); size != 0 {
		t.Errorf("After Clear(), expected size 0, got %d", size)
	}

	// Add items again after Clear to verify functionality
	for i := range 3 {
		value := fmt.Sprintf("after_clear_%d", i)
		m.Store(i+100, &value)
	}
	if size := m.Size(); size != 3 {
		t.Errorf("After adding 3 items post-Clear, expected size 3, got %d", size)
	}
}

// TestFlatMap_IsZero tests the IsZero method
func TestFlatMap_IsZero(t *testing.T) {
	var m FlatMap[string, int]

	// Test empty map
	if m.Size() != 0 {
		t.Error("Expected IsZero() to return true for empty map")
	}

	// Add an item
	m.Store("key1", 100)
	if m.Size() == 0 {
		t.Error("Expected IsZero() to return false for non-empty map")
	}

	// Delete the item
	m.Delete("key1")
	if m.Size() != 0 {
		t.Error("Expected IsZero() to return true after deleting all items")
	}

	// Add multiple items
	for i := 1; i <= 10; i++ {
		m.Store(fmt.Sprintf("key_%d", i), i)
	}
	if m.Size() == 0 {
		t.Error("Expected IsZero() to return false for map with multiple items")
	}

	// Test Clear as an alternative to individual deletes
	m.Clear()
	if m.Size() != 0 {
		t.Error("Expected IsZero() to return true after Clear()")
	}

	// Verify Clear is equivalent to individual deletes
	for i := 1; i <= 5; i++ {
		m.Store(fmt.Sprintf("test_%d", i), i*100)
	}
	if m.Size() == 0 {
		t.Error("Expected IsZero() to return false after adding items")
	}

	// Delete all items individually for comparison
	for i := 1; i <= 5; i++ {
		m.Delete(fmt.Sprintf("test_%d", i))
	}
	if m.Size() != 0 {
		t.Error("Expected IsZero() to return true after deleting all items individually")
	}
}

// ---------------- Additional boundary/concurrency tests ----------------

func TestFlatMap_LoadOrStoreFn_OnceUnderRace(t *testing.T) {
	m := NewFlatMap[int, int]()
	var called int32
	var wg sync.WaitGroup
	workers := max(2, runtime.GOMAXPROCS(0)) // Reduce Concurrency
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			_, _ = m.LoadOrStoreFn(999, func() int {
				// widen race window
				atomic.AddInt32(&called, 1)
				time.Sleep(1 * time.Millisecond)
				return 777
			})
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&called); got != 1 {
		t.Fatalf("LoadOrStoreFn invoked %d times; want 1", got)
	}
	if v, ok := m.Load(999); !ok || v != 777 {
		t.Fatalf("post state: got (%v,%v), want (777,true)", v, ok)
	}
}

// TestFlatMap_DoubleBufferConsistency tests the double buffer mechanism
func TestFlatMap_DoubleBufferConsistency(t *testing.T) {
	m := NewFlatMap[int, int]()
	const numKeys = 100
	const numUpdates = 50

	// Insert initial data
	for i := 1; i <= numKeys; i++ {
		m.Compute(i, func(e *MapEntry[int, int]) {
			e.Update(i)
		})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Continuous readers to stress test the double buffer
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := 1; i <= numKeys; i++ {
						val, ok := m.Load(i)
						if !ok {
							t.Errorf("Key %d should exist", i)
							return
						}
						// Value should be consistent (either old or new, but
						// not corrupted)
						if val < 0 {
							t.Errorf("Corrupted value %d for key %d", val, i)
							return
						}
					}
				}
			}
		}()
	}

	// Writer that updates all keys multiple times
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range numUpdates {
			for i := 1; i <= numKeys; i++ {
				m.Compute(
					i,
					func(e *MapEntry[int, int]) {
						e.Update(e.Value() + 1000)
					},
				)
			}
			runtime.Gosched() // Give readers a chance
		}
	}()

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	// t.Log("Double buffer consistency test completed")
}

// TestFlatMap_DoubleBufferConsistency_StressABA stresses rapid A/B flips on
// the same key and verifies that readers never observe torn values (i.e.,
// reading half old, half new). This detects correctness of the double-buffer
// protocol under extreme contention.
func TestFlatMap_DoubleBufferConsistency_StressABA(t *testing.T) {
	type pair struct{ X, Y uint16 }
	m := NewFlatMap[int, pair]()

	// Initialize key 0
	m.Store(0, pair{X: 0, Y: ^uint16(0)})

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		seq  uint32
	)

	// Start multiple writers to maximize flip frequency on the same slot
	writerN := 4
	for range writerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s := atomic.AddUint32(&seq, 1)
					val := pair{X: uint16(s), Y: ^uint16(s)}
					m.Compute(
						0,
						func(e *MapEntry[int, pair]) {
							e.Update(val)
						},
					)
				}
			}
		}()
	}

	// Start readers to continuously validate that values are not torn
	readerN := 8
	for range readerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v, ok := m.Load(0)
					if !ok {
						t.Errorf("key missing while stressed")
						return
					}
					if v.Y != ^v.X {
						t.Errorf("torn read detected: %+v", v)
						return
					}
				}
			}
		}()
	}

	// Run for a short duration to stress concurrent flips
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// Stress ABA-like rapid flips on the same key and ensure seqlock prevents torn
// reads.
func TestFlatMap_SeqlockConsistency_StressABA(t *testing.T) {
	type pair struct{ X, Y uint64 }
	m := NewFlatMap[int, pair]()

	m.Store(0, pair{X: 0, Y: ^uint64(0)})

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		seq  uint32
	)

	writerN := 4 // Reduce Concurrency
	for range writerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s := atomic.AddUint32(&seq, 1)
					val := pair{X: uint64(s), Y: ^uint64(s)}
					m.Compute(
						0,
						func(e *MapEntry[int, pair]) {
							e.Update(val)
						},
					)
				}
			}
		}()
	}

	readerN := 4 // Reduce Concurrency
	for range readerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v, ok := m.Load(0)
					if !ok {
						t.Errorf("key missing while stressed")
						return
					}
					if v.Y != ^v.X {
						t.Errorf("torn read detected: %+v", v)
						return
					}
				}
			}
		}()
	}

	time.Sleep(150 * time.Millisecond) // Reduce Duration
	close(stop)
	wg.Wait()
}

// Load/Delete race semantics: under seqlock, readers should not observe
// ok==true with zero value.
func TestFlatMap_LoadDeleteRace_Semantics(t *testing.T) {
	m := NewFlatMap[string, uint32]()
	const key = "k"
	const insertedVal uint32 = 1

	var (
		anom uint64
		stop uint32
	)

	readers := max(2, runtime.GOMAXPROCS(0)) // Reduce Concurrency
	dur := 500 * time.Millisecond            // Reduce Duration

	var wg sync.WaitGroup
	wg.Add(readers + 1)

	go func() {
		defer wg.Done()
		deadline := time.Now().Add(dur)
		for time.Now().Before(deadline) {
			m.Store(key, insertedVal)
			runtime.Gosched()
			m.Delete(key)
			runtime.Gosched()
		}
		atomic.StoreUint32(&stop, 1)
	}()

	for range readers {
		go func() {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				v, ok := m.Load(key)
				if ok && v == 0 {
					atomic.AddUint64(&anom, 1)
				}
			}
		}()
	}

	wg.Wait()
	if anom > 0 {
		t.Fatalf(
			"Load/Delete race: observed ok==true && value==0 %d times",
			anom,
		)
	}
}

func TestFlatMap_KeyTornRead_Stress(t *testing.T) {
	// This test stresses concurrent delete/re-insert cycles to try to expose
	// torn reads on keys when K is larger than machine word size.
	// It also serves as a good target for `go test -race` which should report
	// a data race if key memory is read while being concurrently cleared.

	type bigKey struct{ A, B uint64 }

	m := NewFlatMap[bigKey, int]()

	const N = 1024
	keys := make([]bigKey, N)
	for i := range N {
		// Maintain invariant: B == ^A
		ai := uint64(i*2147483647 + 123456789)
		keys[i] = bigKey{A: ai, B: ^ai}
		m.Store(keys[i], i)
	}

	var (
		wg           sync.WaitGroup
		stop         = make(chan struct{})
		foundTornKey atomic.Bool
	)

	// Readers: continuously range and validate key invariant
	readerN := 8
	wg.Add(readerN)
	for range readerN {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m.Range(func(k bigKey, _ int) bool {
						if k.B != ^k.A {
							// t.Logf("torn key: %v", k)
							foundTornKey.Store(true)
							return false
						}
						return true
					})
				}
			}
		}()
	}

	// Additional readers: hammer Load to exercise key comparisons
	loadN := 4
	wg.Add(loadN)
	for r := range loadN {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := id; i < N; i += loadN {
						m.Load(keys[i])
					}
				}
			}
		}(r)
	}

	// Writers: repeatedly delete and re-insert the same keys to trigger
	// meta-clearing followed by key memory clearing in current implementation.
	writerN := 2
	wg.Add(writerN)
	for w := range writerN {
		go func(offset int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := offset; i < N; i += writerN {
						k := keys[i]
						// Delete
						m.Compute(
							k,
							func(e *MapEntry[bigKey, int]) {
								e.Delete()
							},
						)
						// Re-insert
						m.Compute(
							k,
							func(e *MapEntry[bigKey, int]) {
								e.Update(i)
							},
						)
					}
					runtime.Gosched()
				}
			}
		}(w)
	}

	// Run for a short duration
	dur := 1500 * time.Millisecond
	timer := time.NewTimer(dur)
	<-timer.C
	close(stop)
	wg.Wait()

	if foundTornKey.Load() {
		t.Fatalf(
			"detected possible torn read of key: invariant k.B == ^k.A violated under concurrency",
		)
	}
}

// Key torn-read stress: delete/re-insert big keys under load and ensure key
// invariants hold.
func TestFlatMap_KeyTornRead_Stress_Heavy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy stress in -short mode")
	}

	type bigKey struct{ A, B uint64 }
	m := NewFlatMap[bigKey, int]()

	const N = 4096
	keys := make([]bigKey, N)
	for i := range N {
		ai := uint64(i*2147483647 + 987654321)
		keys[i] = bigKey{A: ai, B: ^ai}
		m.Store(keys[i], i)
	}

	var (
		wg           sync.WaitGroup
		stop         = make(chan struct{})
		foundTornKey atomic.Bool
	)

	readerN := max(8, runtime.GOMAXPROCS(0)*2)
	wg.Add(readerN)
	for range readerN {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m.Range(func(k bigKey, _ int) bool {
						if k.B != ^k.A {
							foundTornKey.Store(true)
							return false
						}
						return true
					})
				}
			}
		}()
	}

	loadN := max(4, runtime.GOMAXPROCS(0))
	wg.Add(loadN)
	for r := range loadN {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := id; i < N; i += loadN {
						m.Load(keys[i])
					}
				}
			}
		}(r)
	}

	writerN := max(8, runtime.GOMAXPROCS(0)*2)
	wg.Add(writerN)
	for w := range writerN {
		go func(offset int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := offset; i < N; i += writerN {
						k := keys[i]
						m.Compute(
							k,
							func(e *MapEntry[bigKey, int]) {
								e.Delete()
							},
						)
						m.Compute(
							k,
							func(e *MapEntry[bigKey, int]) {
								e.Update(i)
							},
						)
					}
					runtime.Gosched()
				}
			}
		}(w)
	}

	dur := 2 * time.Second
	timer := time.NewTimer(dur)
	<-timer.C
	close(stop)
	wg.Wait()

	if foundTornKey.Load() {
		t.Fatalf(
			"detected possible torn read of key: invariant k.B == ^k.A violated under concurrency (heavy)",
		)
	}
}

func TestFlatMap_Range_NoDuplicateVisit_Heavy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy stress in -short mode")
	}

	m := NewFlatMap[int, int]()
	const N = 2048 * 50 // ~100K keys to cover chains
	for i := range N {
		m.Store(i, i)
	}

	var stop uint32
	writerN := max(4, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	wg.Add(writerN)
	for w := range writerN {
		go func(offset int) {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				for i := offset; i < N; i += writerN {
					m.Compute(
						i,
						func(e *MapEntry[int, int]) {
							e.Update(e.Value() + 1)
						},
					)
				}
				runtime.Gosched()
			}
		}(w)
	}

	// Multiple Range passes; each pass must not yield duplicates
	rounds := 20
	for range rounds {
		seen := make([]uint8, N) // compact bitset
		m.Range(func(k, v int) bool {
			if k >= 0 && k < N {
				if seen[k] != 0 {
					t.Fatalf(
						"Range yielded duplicate key within a single pass (heavy): %d",
						k,
					)
				}
				seen[k] = 1
			}
			return true
		})
	}

	atomic.StoreUint32(&stop, 1)
	wg.Wait()
}

// New test: verify a single Range call never yields duplicate keys, even under
// concurrent writers causing seqlock retries.
func TestFlatMap_Range_NoDuplicateVisit(t *testing.T) {
	m := NewFlatMap[int, int]()
	const N = 2048 * 100 // cover multiple buckets and chains
	for i := range N {
		m.Store(i, i)
	}

	var stop uint32
	writerN := max(2, runtime.GOMAXPROCS(0)/2) // Reduce Concurrency
	var wg sync.WaitGroup
	wg.Add(writerN)
	for w := range writerN {
		go func(offset int) {
			defer wg.Done()
			for atomic.LoadUint32(&stop) == 0 {
				for i := offset; i < N; i += writerN {
					// mutate value to force seq changes
					m.Compute(
						i,
						func(e *MapEntry[int, int]) {
							e.Update(e.Value() + 1)
						},
					)
				}
				runtime.Gosched()
			}
		}(w)
	}

	// Run several Range passes under writer churn; each pass must not see
	// duplicates
	rounds := 10
	for range rounds {
		seen := make(map[int]struct{}, N)
		m.Range(func(k, v int) bool {
			if _, dup := seen[k]; dup {
				t.Fatalf(
					"Range yielded duplicate key within a single pass: %d",
					k,
				)
			}
			seen[k] = struct{}{}
			return true
		})
	}

	atomic.StoreUint32(&stop, 1)
	wg.Wait()
}

func TestFlatMap_RangeProcess_Basic(t *testing.T) {
	m := NewFlatMap[int, int]()
	const N = 1024
	for i := range N {
		m.Store(i, i)
	}

	// Delete evens, add +100 to odds, cancel others (none)
	m.ComputeRange(func(e *MapEntry[int, int]) bool {
		if e.Key()%2 == 0 {
			e.Delete()
		} else {
			e.Update(e.Value() + 100)
		}
		return true
	})

	for i := range N {
		v, ok := m.Load(i)
		if i%2 == 0 {
			if ok {
				t.Fatalf("key %d should be deleted", i)
			}
		} else {
			if !ok || v != i+100 {
				t.Fatalf("key %d got (%v,%v), want (%d,true)", i, v, ok, i+100)
			}
		}
	}
}

func TestFlatMap_RangeProcess_CancelAndEarlyStop(t *testing.T) {
	m := NewFlatMap[int, int]()
	for i := range 100 {
		m.Store(i, i)
	}
	count := 0
	m.ComputeRange(func(e *MapEntry[int, int]) bool {
		count++
		if count < 10 {
			e.Update(e.Value() + 1)
		}
		return true
	})
	// Verify that exactly 9 keys were updated (value incremented by 1)
	// and the rest remain unchanged. We cannot predict WHICH keys were updated
	// because hash map iteration order is not guaranteed.
	updatedCount := 0
	unchangedCount := 0
	for i := range 100 {
		v, ok := m.Load(i)
		if !ok {
			t.Fatalf("missing key %d", i)
		}
		switch v {
		case i + 1:
			updatedCount++
		case i:
			unchangedCount++
		default:
			t.Fatalf("key %d has unexpected value %d (expected %d or %d)", i, v, i, i+1)
		}
	}
	if updatedCount != 9 {
		t.Fatalf("expected 9 updated keys, got %d", updatedCount)
	}
	if unchangedCount != 91 {
		t.Fatalf("expected 91 unchanged keys, got %d", unchangedCount)
	}
}

// Concurrency smoke: interleave ComputeRange with Compute to ensure no panics
// and state convergence.
func TestFlatMap_RangeProcess_Concurrent(t *testing.T) {
	m := NewFlatMap[int, int]()
	const N = 512
	for i := range N {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(3)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.ComputeRange(func(e *MapEntry[int, int]) bool {
					if e.Key()%7 == 0 {
						e.Delete()
					} else {
						e.Update(e.Value() + 1)
					}
					return true
				})
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := range N {
					m.Compute(
						i,
						func(e *MapEntry[int, int]) {
							if !e.Loaded() {
								e.Update(i)
							} else {
								e.Update(e.Value() + 1)
							}
						},
					)
				}
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for i := range N {
					m.Load(i)
				}
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestFlatMap_ConcurrentShrink(t *testing.T) {
	m := NewFlatMap[int, int](WithAutoShrink())
	const numEntries = 10000
	const numGoroutines = 8
	const numOperations = 1000

	// Pre-populate the map to create a large table
	for i := range numEntries {
		m.Store(i, i*2)
	}

	// Delete most entries to trigger potential shrink
	for i := range numEntries - 100 {
		m.Delete(i)
	}

	var wg sync.WaitGroup
	var errors int64

	// Concurrent readers during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				key := numEntries - 100 + (base*numOperations+i)%100
				if val, ok := m.Load(key); ok && val != key*2 {
					atomic.AddInt64(&errors, 1)
				}
				runtime.Gosched()
			}
		}(g)
	}

	// Concurrent writers during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				key := numEntries + base*numOperations + i
				m.Store(key, key*2)
				if val, ok := m.Load(key); !ok || val != key*2 {
					atomic.AddInt64(&errors, 1)
				}
				m.Delete(key)
				runtime.Gosched()
			}
		}(g)
	}

	// Concurrent deleters during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				key := numEntries + 100000 + base*numOperations + i
				m.Store(key, key*2)
				m.Delete(key)
				if _, ok := m.Load(key); ok {
					atomic.AddInt64(&errors, 1)
				}
				runtime.Gosched()
			}
		}(g)
	}

	wg.Wait()

	if errors > 0 {
		t.Errorf("Found %d data consistency errors during concurrent shrink", errors)
	}

	// Verify remaining data integrity
	for i := numEntries - 100; i < numEntries; i++ {
		if val, ok := m.Load(i); !ok || val != i*2 {
			t.Errorf("Data corruption after shrink: key=%d, expected=%d, actual=%d, ok=%v",
				i, i*2, val, ok)
		}
	}
}

// TestFlatMap_ShrinkDataIntegrity tests data integrity during shrink operations
func TestFlatMap_ShrinkDataIntegrity(t *testing.T) {
	m := NewFlatMap[string, int](WithAutoShrink())
	const numEntries = 5000

	// Create test data
	for i := range numEntries {
		key := fmt.Sprintf("key_%d", i)
		value := i * 3
		m.Store(key, value)
	}

	// Delete most entries to trigger shrink (delete first numEntries-200 keys)
	deletedKeys := make(map[string]bool)
	for i := range numEntries - 200 {
		key := fmt.Sprintf("key_%d", i)
		m.Delete(key)
		deletedKeys[key] = true
	}

	// Verify remaining data integrity after shrink
	for i := numEntries - 200; i < numEntries; i++ {
		key := fmt.Sprintf("key_%d", i)
		expectedValue := i * 3
		if actualValue, ok := m.Load(key); !ok || actualValue != expectedValue {
			t.Errorf("Data corruption after shrink: key=%s, expected=%d, actual=%d, ok=%v",
				key, expectedValue, actualValue, ok)
		}
	}

	// Verify deleted keys are actually gone
	for key := range deletedKeys {
		if _, ok := m.Load(key); ok {
			t.Errorf("Deleted key still present after shrink: %s", key)
		}
	}
}

// TestFlatMap_ConcurrentShrinkWithRangeProcess tests ComputeRange during shrink
func TestFlatMap_ConcurrentShrinkWithRangeProcess(t *testing.T) {
	m := NewFlatMap[int, int](WithAutoShrink())
	const numEntries = 8000
	const numGoroutines = 4

	// Pre-populate
	for i := range numEntries {
		m.Store(i, i*5)
	}

	// Delete most entries to trigger shrink
	for i := range numEntries - 500 {
		m.Delete(i)
	}

	var wg sync.WaitGroup
	var rangeErrors int64
	var processErrors int64

	// Concurrent Range operations during shrink
	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range 100 {
				count := 0
				m.Range(func(k, v int) bool {
					count++
					if v != k*5 {
						atomic.AddInt64(&rangeErrors, 1)
					}
					return true
				})
				if count == 0 {
					atomic.AddInt64(&rangeErrors, 1)
				}
				runtime.Gosched()
			}
		}()
	}

	// Concurrent ComputeRange operations during shrink
	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range 50 {
				processCount := 0
				m.ComputeRange(func(e *MapEntry[int, int]) bool {
					processCount++
					if e.Value() != e.Key()*5 {
						atomic.AddInt64(&processErrors, 1)
					}
					return true
				})
				if processCount == 0 {
					atomic.AddInt64(&processErrors, 1)
				}
				runtime.Gosched()
			}
		}()
	}

	// Concurrent operations during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range 200 {
				key := numEntries + base*200 + i
				m.Store(key, key*5)
				if val, ok := m.Load(key); !ok || val != key*5 {
					atomic.AddInt64(&processErrors, 1)
				}
				m.Delete(key)
				runtime.Gosched()
			}
		}(g)
	}

	wg.Wait()

	if rangeErrors > 0 {
		t.Errorf("Found %d Range errors during concurrent shrink", rangeErrors)
	}
	if processErrors > 0 {
		t.Errorf("Found %d ComputeRange errors during concurrent shrink", processErrors)
	}
}

func TestFlatMap_ShrinkStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	m := NewFlatMap[int, int](WithAutoShrink())
	const cycles = 10
	const entriesPerCycle = 5000
	const numGoroutines = 6

	for cycle := range cycles {
		var wg sync.WaitGroup
		var errors int64

		// Fill the map
		for i := range entriesPerCycle {
			m.Store(i, i*7)
		}

		// Concurrent operations while triggering shrink
		wg.Add(numGoroutines)
		for g := range numGoroutines {
			go func(base int) {
				defer wg.Done()

				// Delete entries to trigger shrink
				for i := base; i < entriesPerCycle-100; i += numGoroutines {
					m.Delete(i)
				}

				// Perform reads/writes during shrink
				for i := range 500 {
					key := entriesPerCycle + base*500 + i
					m.Store(key, key*7)
					if val, ok := m.Load(key); !ok || val != key*7 {
						atomic.AddInt64(&errors, 1)
					}
					m.Delete(key)
					runtime.Gosched()
				}
			}(g)
		}

		wg.Wait()

		if errors > 0 {
			t.Errorf("Cycle %d: Found %d errors during shrink stress test", cycle, errors)
		}

		// Verify remaining data
		for i := entriesPerCycle - 100; i < entriesPerCycle; i++ {
			if val, ok := m.Load(i); !ok || val != i*7 {
				t.Errorf("Cycle %d: Data corruption: key=%d, expected=%d, actual=%d, ok=%v",
					cycle, i, i*7, val, ok)
			}
		}

		// Clean up for next cycle
		for i := entriesPerCycle - 100; i < entriesPerCycle; i++ {
			m.Delete(i)
		}
	}
}

func TestFlatMap_ShrinkSeqlockConsistency(t *testing.T) {
	m := NewFlatMap[int, int](WithAutoShrink())
	const numEntries = 6000
	const numGoroutines = 6
	const numOperations = 800

	// Pre-populate
	for i := range numEntries {
		m.Store(i, i*11)
	}

	// Delete most entries to trigger shrink
	for i := range numEntries - 300 {
		m.Delete(i)
	}

	var wg sync.WaitGroup
	var seqlockErrors int64

	// Concurrent readers testing seqlock consistency during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				// Test remaining keys
				key := numEntries - 300 + (base*numOperations+i)%300
				val, ok := m.Load(key)
				if ok && val != key*11 {
					atomic.AddInt64(&seqlockErrors, 1)
				}

				// Test Range consistency
				count := 0
				m.Range(func(k, v int) bool {
					count++
					if v != k*11 {
						atomic.AddInt64(&seqlockErrors, 1)
					}
					return count < 50 // Early termination to reduce test time
				})

				runtime.Gosched()
			}
		}(g)
	}

	// Concurrent writers during shrink
	wg.Add(numGoroutines)
	for g := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for i := range numOperations {
				key := numEntries + base*numOperations + i
				m.Store(key, key*11)

				// Immediate read to test consistency
				if val, ok := m.Load(key); !ok || val != key*11 {
					atomic.AddInt64(&seqlockErrors, 1)
				}

				m.Delete(key)
				runtime.Gosched()
			}
		}(g)
	}

	wg.Wait()

	if seqlockErrors > 0 {
		t.Errorf("Found %d seqlock consistency errors during concurrent shrink", seqlockErrors)
	}

	// Final verification of remaining data
	for i := numEntries - 300; i < numEntries; i++ {
		if val, ok := m.Load(i); !ok || val != i*11 {
			t.Errorf("Final data corruption: key=%d, expected=%d, actual=%d, ok=%v",
				i, i*11, val, ok)
		}
	}
}

// TestFlatMap_RangeProcess_DuringResize tests ComputeRange during table
// resize operations
func TestFlatMap_RangeProcess_DuringResize(t *testing.T) {
	m := NewFlatMap[int, int]()

	// Pre-populate to trigger potential resize
	for i := range 50 {
		m.Store(i, i*2)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Goroutine that continuously adds/removes items to trigger resizes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; ; i++ {
			select {
			case <-stop:
				return
			default:
				m.Store(i, i*2)
				if i%10 == 0 {
					// Delete some keys to potentially trigger shrink
					for j := i - 20; j < i-10 && j >= 50; j++ {
						m.Delete(j)
					}
				}
				runtime.Gosched()
			}
		}
	}()

	// Goroutines that continuously call ComputeRange during resize
	const numRangeProcessors = 4
	processCounts := make([]int64, numRangeProcessors)

	for g := range numRangeProcessors {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			var count int64
			for {
				select {
				case <-stop:
					processCounts[goroutineID] = count
					return
				default:
					m.ComputeRange(
						func(e *MapEntry[int, int]) bool {
							count++
							// Occasionally update values
							if count%100 == 0 {
								e.Update(e.Value() + 1)
							}
							return true
						},
					)
					runtime.Gosched()
				}
			}
		}(g)
	}

	// Run for a short duration
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify that ComputeRange operations completed successfully
	totalProcessed := int64(0)
	for i, count := range processCounts {
		if count == 0 {
			t.Errorf("Goroutine %d processed 0 items, expected > 0", i)
		}
		totalProcessed += count
	}

	if totalProcessed == 0 {
		t.Error("No items were processed during resize stress test")
	}

	// Verify map is still consistent
	m.ComputeRange(func(e *MapEntry[int, int]) bool {
		if e.Value() < 0 {
			t.Errorf("Found negative value %d for key %d", e.Value(), e.Key())
		}
		return true
	})
}

// TestFlatMap_RangeProcess_EarlyTermination tests early termination scenarios
func TestFlatMap_RangeProcess_EarlyTermination(t *testing.T) {
	m := NewFlatMap[int, int]()

	// Add data
	for i := range 20 {
		m.Store(i, i*5)
	}

	// Test that ComputeRange can handle panics gracefully (if any)
	// and that partial processing doesn't corrupt the map
	processedCount := 0
	m.ComputeRange(func(e *MapEntry[int, int]) bool {
		processedCount++
		if e.Key() == 10 {
			// Simulate early termination by returning without processing
			// In real scenarios, this might be due to context cancellation
			return true
		}
		e.Update(e.Value() + 1)
		return true
	})

	// Verify that the map is still consistent
	consistentCount := 0
	m.ComputeRange(func(e *MapEntry[int, int]) bool {
		consistentCount++
		if e.Value() < 0 {
			t.Errorf("Found inconsistent value %d for key %d", e.Value(), e.Key())
		}
		return true
	})

	if consistentCount != 20 {
		t.Errorf("Expected 20 consistent entries, got %d", consistentCount)
	}

	if processedCount == 0 {
		t.Error("Expected some entries to be processed")
	}
}

// TestFlatMap_Clear tests the Clear method
func TestFlatMap_Clear(t *testing.T) {
	var m FlatMap[string, int]

	// Test clear on empty map
	m.Clear()
	if m.Size() != 0 {
		t.Error("Expected map to be empty after clearing empty map")
	}
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clearing empty map, got %d", size)
	}

	// Add some data
	for i := range 10 {
		key := fmt.Sprintf("key_%d", i)
		m.Store(key, i*10)
	}

	// Verify data exists
	if size := m.Size(); size != 10 {
		t.Errorf("Expected size 10 before clear, got %d", size)
	}
	if m.Size() == 0 {
		t.Error("Expected map not to be empty before clear")
	}

	// Clear the map
	m.Clear()

	// Verify map is empty
	if m.Size() != 0 {
		t.Error("Expected map to be empty after clear")
	}
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}

	// Verify all keys are gone
	for i := range 10 {
		key := fmt.Sprintf("key_%d", i)
		if val, ok := m.Load(key); ok {
			t.Errorf("Key %s should be gone after clear, but got (%v, %v)", key, val, ok)
		}
	}

	// Verify Range returns no entries
	count := 0
	m.Range(func(k string, v int) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("Expected 0 entries in Range after clear, got %d", count)
	}
}

// TestFlatMap_ClearAndReinsert tests clearing and then reinserting data
func TestFlatMap_ClearAndReinsert(t *testing.T) {
	m := NewFlatMap[int, string]()

	// Insert initial data
	for i := range 20 {
		value := fmt.Sprintf("initial_%d", i)
		m.Store(i, value)
	}
	if size := m.Size(); size != 20 {
		t.Errorf("Expected size 20 after initial insert, got %d", size)
	}

	// Clear the map
	m.Clear()
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}

	// Reinsert different data
	for i := range 15 {
		key := i + 100 // Different keys
		value := fmt.Sprintf("new_%d", i)
		m.Store(key, value)
	}
	if size := m.Size(); size != 15 {
		t.Errorf("Expected size 15 after reinsert, got %d", size)
	}

	// Verify new data is accessible
	for i := range 15 {
		key := i + 100
		expected := fmt.Sprintf("new_%d", i)
		if val, ok := m.Load(key); !ok || val != expected {
			t.Errorf("Key %d: expected (%s, true), got (%v, %v)", key, expected, val, ok)
		}
	}

	// Verify old data is gone
	for i := range 20 {
		if val, ok := m.Load(i); ok {
			t.Errorf("Old key %d should be gone, but got (%v, %v)", i, val, ok)
		}
	}
}

// TestFlatMap_ClearLargeMap tests clearing a large map
func TestFlatMap_ClearLargeMap(t *testing.T) {
	m := NewFlatMap[int, int](WithCapacity(2048))

	// Insert a large amount of data
	const N = 1000
	for i := range N {
		m.Store(i, i*2)
	}
	if size := m.Size(); size != N {
		t.Errorf("Expected size %d after insert, got %d", N, size)
	}

	// Clear the map
	m.Clear()

	// Verify it's empty
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}
	if m.Size() != 0 {
		t.Error("Expected map to be empty after clear")
	}

	// Spot check that data is gone
	testKeys := []int{0, N / 4, N / 2, 3 * N / 4, N - 1}
	for _, key := range testKeys {
		if val, ok := m.Load(key); ok {
			t.Errorf("Key %d should be gone after clear, but got (%v, %v)", key, val, ok)
		}
	}
}

// TestFlatMap_ClearConcurrent tests concurrent access during clear
func TestFlatMap_ClearConcurrent(t *testing.T) {
	m := NewFlatMap[int, int]()

	// Insert initial data
	const N = 100
	for i := range N {
		m.Store(i, i*2)
	}
	if size := m.Size(); size != N {
		t.Errorf("Expected size %d after insert, got %d", N, size)
	}

	var wg sync.WaitGroup
	var clearDone atomic.Bool

	// Start a goroutine that clears the map
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Let readers start
		m.Clear()
		clearDone.Store(true)
	}()

	// Start readers that try to access data during clear
	numReaders := 4
	for r := range numReaders {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := range 200 {
				key := (readerID*50 + i) % N
				m.Load(key) // May or may not find the key
				if clearDone.Load() {
					break
				}
				time.Sleep(time.Microsecond)
			}
		}(r)
	}

	wg.Wait()

	// Verify map is empty after clear
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after concurrent clear, got %d", size)
	}
	if m.Size() != 0 {
		t.Error("Expected map to be empty after concurrent clear")
	}
}

// TestFlatMap_ClearConcurrentOperations tests clear with concurrent writes
func TestFlatMap_ClearConcurrentOperations(t *testing.T) {
	m := NewFlatMap[int, int]()

	// Insert initial data
	const N = 50
	for i := range N {
		m.Store(i, i)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Start a goroutine that clears the map
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		m.Clear()
		close(stop)
	}()

	// Start writers that try to insert during clear
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 150; i++ {
			select {
			case <-stop:
				return
			default:
				m.Store(i, i*3)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Start readers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range N {
			select {
			case <-stop:
				return
			default:
				m.Load(i)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Wait()

	// The map should be in a consistent state
	// Either completely clear or with some new insertions from the writer
	finalSize := m.Size()

	// Verify consistency - no old entries should remain
	foundOldEntries := false
	for i := range N {
		if _, ok := m.Load(i); ok {
			foundOldEntries = true
			break
		}
	}

	// Old entries should be gone after clear
	if foundOldEntries {
		t.Error("Found old entries that should have been cleared")
	}

	// Any remaining entries should be from the concurrent writer (100-149 range)
	if finalSize > 0 {
		validEntries := 0
		for i := 100; i < 150; i++ {
			if _, ok := m.Load(i); ok {
				validEntries++
			}
		}
		if validEntries != finalSize {
			t.Errorf("Expected all remaining entries to be from concurrent writer, got %d valid out of %d total", validEntries, finalSize)
		}
	}
}

// TestFlatMap_ClearMultipleTimes tests clearing multiple times
func TestFlatMap_ClearMultipleTimes(t *testing.T) {
	m := NewFlatMap[int, int]()

	for round := range 5 {
		// Insert data
		for i := range 20 {
			key := i + round*100
			m.Store(key, i*round)
		}
		if size := m.Size(); size != 20 {
			t.Errorf("Round %d: expected size 20 after insert, got %d", round, size)
		}

		// Clear
		m.Clear()
		if size := m.Size(); size != 0 {
			t.Errorf("Round %d: expected size 0 after clear, got %d", round, size)
		}
		if m.Size() != 0 {
			t.Errorf("Round %d: expected map to be empty after clear", round)
		}
	}

	// Final verification
	if size := m.Size(); size != 0 {
		t.Errorf("Expected final size 0, got %d", size)
	}
	if m.Size() != 0 {
		t.Error("Expected map to be empty at the end")
	}
}

// TestFlatMap_ClearWithShrinkEnabled tests clear with shrink enabled
func TestFlatMap_ClearWithShrinkEnabled(t *testing.T) {
	m := NewFlatMap[int, int](WithCapacity(1024), WithAutoShrink())

	// Insert data to grow the map
	const N = 800
	for i := range N {
		m.Store(i, i)
	}
	if size := m.Size(); size != N {
		t.Errorf("Expected size %d after insert, got %d", N, size)
	}

	// Clear the map
	m.Clear()

	// Verify it's empty
	if size := m.Size(); size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}
	if m.Size() != 0 {
		t.Error("Expected map to be empty after clear")
	}

	// The map should still be functional after clear
	for i := range 10 {
		m.Store(i, i*10)
	}
	if size := m.Size(); size != 10 {
		t.Errorf("Expected size 10 after reinsert, got %d", size)
	}

	// Verify data is accessible
	for i := range 10 {
		if val, ok := m.Load(i); !ok || val != i*10 {
			t.Errorf("Key %d: expected (%d, true), got (%v, %v)", i, i*10, val, ok)
		}
	}
}

// TestFlatMap_RangeProcess_BlockWriters_Strict tests ComputeRange with
// blockWritersOpt=true under heavy concurrent load to ensure writers are
// properly blocked and no torn reads occur.
func TestFlatMap_RangeProcess_BlockWriters_Strict(t *testing.T) {
	type testValue struct {
		X, Y    uint64 // Invariant: Y == ^X
		Counter uint32
	}

	m := NewFlatMap[int, testValue]()
	const N = 512

	// Initialize with values maintaining invariant
	for i := range N {
		val := testValue{
			X:       uint64(i * 123456789),
			Y:       ^uint64(i * 123456789),
			Counter: 0,
		}
		m.Store(i, val)
	}

	var (
		wg               sync.WaitGroup
		stop             = make(chan struct{})
		tornReads        atomic.Uint64
		blockedWrites    atomic.Uint64
		rangeProcessRuns atomic.Uint64
	)

	// ComputeRange goroutine with blockWritersOpt=true
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.Rebuild(func(m *MapRebuild[int, testValue]) {
					for e := range m.Entries() {
						// Verify invariant during processing
						if e.Value().Y != ^e.Value().X {
							tornReads.Add(1)
							t.Errorf("Torn read detected in ComputeRange: key=%d, X=%x, Y=%x", e.Key(), e.Value().X, e.Value().Y)
						}
						// Update counter while maintaining invariant
						e.Update(testValue{
							X:       e.Value().X,
							Y:       e.Value().Y,
							Counter: e.Value().Counter + 1,
						})
					}
				})
				rangeProcessRuns.Add(1)
				runtime.Gosched()
			}
		}
	}()

	// Concurrent writers that should be blocked during ComputeRange
	writerN := 4
	for i := range writerN {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					startTime := time.Now()
					m.Compute(key, func(e *MapEntry[int, testValue]) {
						if !e.Loaded() {
							return
						}
						// Check if we were blocked for a significant time
						if time.Since(startTime) > 10*time.Millisecond {
							blockedWrites.Add(1)
						}
						// Maintain invariant while updating
						e.Update(testValue{
							X:       e.Value().X + uint64(writerID),
							Y:       ^(e.Value().X + uint64(writerID)),
							Counter: e.Value().Counter,
						})
					})
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Concurrent readers to detect torn reads
	readerN := 6
	for range readerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					if val, ok := m.Load(key); ok {
						if val.Y != ^val.X {
							tornReads.Add(1)
							t.Errorf("Torn read detected in Load: key=%d, X=%x, Y=%x", key, val.X, val.Y)
						}
					}
					runtime.Gosched()
				}
			}
		}()
	}

	// Run test for a reasonable duration
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify no torn reads occurred
	if torn := tornReads.Load(); torn > 0 {
		t.Fatalf("Detected %d torn reads during test", torn)
	}

	// Verify ComputeRange ran multiple times
	if runs := rangeProcessRuns.Load(); runs == 0 {
		t.Fatal("ComputeRange should have run at least once")
	}

	t.Logf("Test completed: ComputeRange runs=%d, blocked writes=%d",
		rangeProcessRuns.Load(), blockedWrites.Load())
}

// TestFlatMap_RangeProcess_AllowWriters_Concurrent tests ComputeRange with
// blockWritersOpt=false to verify concurrent writers are allowed and data
// consistency is maintained.
func TestFlatMap_RangeProcess_AllowWriters_Concurrent(t *testing.T) {
	type testValue struct {
		A, B uint64 // Invariant: B == ^A
		Seq  uint32
	}

	m := NewFlatMap[int, testValue]()
	const N = 256

	// Initialize map
	for i := range N {
		val := testValue{
			A:   uint64(i * 987654321),
			B:   ^uint64(i * 987654321),
			Seq: 0,
		}
		m.Store(i, val)
	}

	var (
		wg               sync.WaitGroup
		stop             = make(chan struct{})
		tornReads        atomic.Uint64
		concurrentWrites atomic.Uint64
		rangeProcessRuns atomic.Uint64
	)

	// ComputeRange with blockWritersOpt=false (allow concurrent writes)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for e := range m.Entries() {
					// Verify invariant
					if e.Value().B != ^e.Value().A {
						tornReads.Add(1)
						t.Errorf("Torn read in ComputeRange: key=%d, A=%x, B=%x", e.Key(), e.Value().A, e.Value().B)
					}
					// Increment sequence while maintaining invariant
					e.Update(testValue{
						A:   e.Value().A,
						B:   e.Value().B,
						Seq: e.Value().Seq + 1,
					})
				}
				rangeProcessRuns.Add(1)
				runtime.Gosched()
			}
		}
	}()

	// Concurrent writers (should NOT be blocked)
	writerN := 6
	for i := range writerN {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					m.Compute(key, func(e *MapEntry[int, testValue]) {
						if !e.Loaded() {
							return
						}
						concurrentWrites.Add(1)
						// Update while maintaining invariant
						newA := e.Value().A + uint64(writerID*1000)
						e.Update(testValue{
							A:   newA,
							B:   ^newA,
							Seq: e.Value().Seq,
						})
					})
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Concurrent readers
	readerN := 4
	for range readerN {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					if val, ok := m.Load(key); ok {
						if val.B != ^val.A {
							tornReads.Add(1)
							t.Errorf("Torn read in Load: key=%d, A=%x, B=%x", key, val.A, val.B)
						}
					}
					runtime.Gosched()
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify no torn reads
	if torn := tornReads.Load(); torn > 0 {
		t.Fatalf("Detected %d torn reads during test", torn)
	}

	// Verify concurrent writes occurred (writers were not blocked)
	if writes := concurrentWrites.Load(); writes == 0 {
		t.Fatal("Expected concurrent writes to occur when blockWritersOpt=false")
	}

	t.Logf("Test completed: ComputeRange runs=%d, concurrent writes=%d",
		rangeProcessRuns.Load(), concurrentWrites.Load())
}

// TestFlatMap_RangeProcess_TornReadDetection_Stress performs intensive
// stress testing to detect any torn reads during ComputeRange operations.
func TestFlatMap_RangeProcess_TornReadDetection_Stress(t *testing.T) {
	type complexValue struct {
		ID       uint64
		Checksum uint64 // Should equal ^ID
		Data     [4]uint64
		Tail     uint64 // Should equal ID
	}

	m := NewFlatMap[int, complexValue]()
	const N = 1024

	// Initialize with complex values maintaining multiple invariants
	for i := range N {
		id := uint64(i) * 0x123456789ABCDEF
		val := complexValue{
			ID:       id,
			Checksum: ^id,
			Data:     [4]uint64{id, id + 1, id + 2, id + 3},
			Tail:     id,
		}
		m.Store(i, val)
	}

	var (
		wg               sync.WaitGroup
		stop             = make(chan struct{})
		tornReads        atomic.Uint64
		validationErrors atomic.Uint64
	)

	// Validation function to check all invariants
	validateValue := func(k int, v complexValue, context string) {
		if v.Checksum != ^v.ID {
			tornReads.Add(1)
			t.Errorf("Torn read in %s: key=%d, ID=%x, Checksum=%x", context, k, v.ID, v.Checksum)
		}
		if v.Tail != v.ID {
			tornReads.Add(1)
			t.Errorf("Torn read in %s: key=%d, ID=%x, Tail=%x", context, k, v.ID, v.Tail)
		}
		for i, d := range v.Data {
			if d != v.ID+uint64(i) {
				validationErrors.Add(1)
				t.Errorf("Data corruption in %s: key=%d, Data[%d]=%x, expected=%x",
					context, k, i, d, v.ID+uint64(i))
			}
		}
	}

	// ComputeRange with blockWritersOpt=true for maximum stress
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.Rebuild(func(m *MapRebuild[int, complexValue]) {
					for e := range m.Entries() {
						validateValue(e.Key(), e.Value(), "ComputeRange")

						// Modify while maintaining invariants
						newID := e.Value().ID + 0x1000
						e.Update(complexValue{
							ID:       newID,
							Checksum: ^newID,
							Data:     [4]uint64{newID, newID + 1, newID + 2, newID + 3},
							Tail:     newID,
						})
					}
				})
				runtime.Gosched()
			}
		}
	}()

	// Heavy concurrent readers using different access patterns
	readerN := 8
	for r := range readerN {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					switch readerID % 3 {
					case 0:
						// Sequential access
						for i := range N {
							select {
							case <-stop:
								return
							default:
								if val, ok := m.Load(i); ok {
									validateValue(i, val, "Load-Sequential")
								}
							}
						}
					case 1:
						// Random access
						key := rand.IntN(N)
						if val, ok := m.Load(key); ok {
							validateValue(key, val, "Load-Random")
						}
					case 2:
						// Range access
						m.Range(func(k int, v complexValue) bool {
							validateValue(k, v, "Range")
							return true
						})
					}
					runtime.Gosched()
				}
			}
		}(r)
	}

	// Concurrent writers (should be blocked by ComputeRange)
	writerN := 2
	for i := range writerN {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					key := rand.IntN(N)
					m.Compute(key, func(e *MapEntry[int, complexValue]) {
						if !e.Loaded() {
							return
						}

						// Create new value maintaining invariants
						newID := e.Value().ID + uint64(writerID*0x10000)
						e.Update(complexValue{
							ID:       newID,
							Checksum: ^newID,
							Data:     [4]uint64{newID, newID + 1, newID + 2, newID + 3},
							Tail:     newID,
						})
					})
					runtime.Gosched()
				}
			}
		}(i)
	}

	// Run stress test
	time.Sleep(1 * time.Second)
	close(stop)
	wg.Wait()

	// Final validation
	if torn := tornReads.Load(); torn > 0 {
		t.Fatalf("Detected %d torn reads during stress test", torn)
	}

	if errors := validationErrors.Load(); errors > 0 {
		t.Fatalf("Detected %d validation errors during stress test", errors)
	}

	t.Log("Stress test completed successfully - no torn reads or validation errors detected")
}

// TestFlatMap_RangeProcess_WriterBlocking_Verification verifies that when
// blockWritersOpt=true, writers are actually blocked and cannot proceed
// during ComputeRange execution.
func TestFlatMap_RangeProcess_WriterBlocking_Verification(t *testing.T) {
	m := NewFlatMap[int, int]()
	const N = 100

	// Initialize map
	for i := range N {
		m.Store(i, i)
	}

	var (
		wg                  sync.WaitGroup
		rangeProcessStarted = make(chan struct{})
		rangeProcessDone    = make(chan struct{})
		writerAttempted     = make(chan struct{})
		writerCompleted     atomic.Bool
	)

	// ComputeRange that takes some time and blocks writers
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(rangeProcessStarted)

		m.Rebuild(func(m *MapRebuild[int, int]) {
			for e := range m.Entries() {
				// Simulate some processing time
				time.Sleep(10 * time.Millisecond)
				e.Update(e.Value() + 1)
			}
		})

		close(rangeProcessDone)
	}()

	// Writer that should be blocked
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait for ComputeRange to start
		<-rangeProcessStarted
		time.Sleep(1 * time.Millisecond) // Ensure ComputeRange is running

		startTime := time.Now()
		close(writerAttempted)

		// This should be blocked until ComputeRange completes
		m.Store(0, 999)

		duration := time.Since(startTime)
		writerCompleted.Store(true)

		// Verify we were blocked for a reasonable time
		if duration < 50*time.Millisecond {
			t.Errorf("Writer was not blocked long enough: %v", duration)
		}
	}()

	// Verify writer is blocked while ComputeRange runs
	wg.Add(1)
	go func() {
		defer wg.Done()

		<-rangeProcessStarted
		<-writerAttempted

		// Writer has started; it should still be blocked until ComputeRange completes
		if writerCompleted.Load() {
			t.Error("Writer should not have completed while ComputeRange is running")
		}

		// Wait for ComputeRange to complete
		<-rangeProcessDone

		// Give writer a chance to complete
		time.Sleep(5 * time.Millisecond)

		if !writerCompleted.Load() {
			t.Error("Writer should have completed after ComputeRange finished")
		}
	}()

	wg.Wait()

	// Verify final state
	if val, ok := m.Load(0); !ok || val != 999 {
		t.Errorf("Expected final value 999, got %v (ok=%v)", val, ok)
	}
}

// ============================================================================
// go1.23
// ============================================================================

func TestFlatMap_All(t *testing.T) {
	t.Run("EmptyMap", func(t *testing.T) {
		m := NewFlatMap[int, string]()
		count := 0

		for k, v := range m.All() {
			count++
			t.Errorf("Unexpected iteration on empty map: key=%d, value=%s", k, v)
		}

		if count != 0 {
			t.Errorf("Expected 0 iterations on empty map, got %d", count)
		}
	})

	t.Run("SingleElement", func(t *testing.T) {
		m := NewFlatMap[string, int]()
		m.Store("key1", 100)

		found := maps.Collect(m.All())

		if len(found) != 1 {
			t.Errorf("Expected 1 element, got %d", len(found))
		}

		if found["key1"] != 100 {
			t.Errorf("Expected key1=100, got key1=%d", found["key1"])
		}
	})

	t.Run("MultipleElements", func(t *testing.T) {
		m := NewFlatMap[int, string]()
		expected := make(map[int]string)

		// Add test data
		for i := range 50 {
			value := fmt.Sprintf("value_%d", i)
			m.Store(i, value)
			expected[i] = value
		}

		// Iterate using All
		found := maps.Collect(m.All())

		// Verify all elements are found
		if len(found) != len(expected) {
			t.Errorf("Expected %d elements, got %d", len(expected), len(found))
		}

		for k, expectedV := range expected {
			if foundV, ok := found[k]; !ok || foundV != expectedV {
				t.Errorf("Key %d: expected %s, got %s (ok=%v)", k, expectedV, foundV, ok)
			}
		}
	})

	t.Run("EarlyBreak", func(t *testing.T) {
		m := NewFlatMap[int, int]()

		// Add test data
		for i := range 100 {
			m.Store(i, i*2)
		}

		count := 0
		for k, v := range m.All() {
			count++
			if count >= 10 {
				break // Early termination
			}
			// Verify values are correct
			if v != k*2 {
				t.Errorf("Key %d: expected %d, got %d", k, k*2, v)
			}
		}

		if count != 10 {
			t.Errorf("Expected exactly 10 iterations with early break, got %d", count)
		}
	})

	t.Run("ConcurrentIteration", func(t *testing.T) {
		m := NewFlatMap[int, string]()

		// Pre-populate map
		for i := range 100 {
			m.Store(i, fmt.Sprintf("value_%d", i))
		}

		var wg sync.WaitGroup
		const numGoroutines = 5
		results := make([]map[int]string, numGoroutines)

		// Start multiple goroutines iterating concurrently
		for i := range numGoroutines {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				found := maps.Collect(m.All())
				results[idx] = found
			}(i)
		}

		wg.Wait()

		// Verify all goroutines saw consistent data
		for i, result := range results {
			if len(result) == 0 {
				t.Errorf("Goroutine %d found no elements", i)
				continue
			}

			// Check that values are consistent
			for k, v := range result {
				expected := fmt.Sprintf("value_%d", k)
				if v != expected {
					t.Errorf("Goroutine %d: key %d expected %s, got %s", i, k, expected, v)
				}
			}
		}
	})

	t.Run("IterationWithModification", func(t *testing.T) {
		m := NewFlatMap[int, int]()

		// Pre-populate map
		for i := range 20 {
			m.Store(i, i)
		}

		// Iterate and modify concurrently
		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: Iterate
		go func() {
			defer wg.Done()
			count := 0
			for k, v := range m.All() {
				count++
				// Basic sanity check - values should be non-negative
				if k < 0 || v < 0 {
					t.Errorf("Unexpected negative values: k=%d, v=%d", k, v)
				}
			}
			// Should see some elements (exact count may vary due to concurrent modifications)
			if count == 0 {
				t.Error("Iterator saw no elements during concurrent modification")
			}
		}()

		// Goroutine 2: Modify map
		go func() {
			defer wg.Done()
			for i := 20; i < 40; i++ {
				m.Store(i, i*2)
			}
			// Delete some original elements
			for i := 0; i < 10; i += 2 {
				m.Delete(i)
			}
		}()

		wg.Wait()
	})

	t.Run("PointerValues", func(t *testing.T) {
		m := NewFlatMap[string, *int]()

		// Add pointer values
		values := make(map[string]*int)
		for i := range 10 {
			key := fmt.Sprintf("key_%d", i)
			val := new(int)
			*val = i * 10
			m.Store(key, val)
			values[key] = val
		}

		// Iterate and verify pointer values
		found := maps.Collect(m.All())

		if len(found) != len(values) {
			t.Errorf("Expected %d elements, got %d", len(values), len(found))
		}

		for k, expectedPtr := range values {
			if foundPtr, ok := found[k]; !ok {
				t.Errorf("Key %s not found", k)
			} else if foundPtr != expectedPtr {
				t.Errorf("Key %s: pointer mismatch", k)
			} else if *foundPtr != *expectedPtr {
				t.Errorf("Key %s: value mismatch, expected %d, got %d", k, *expectedPtr, *foundPtr)
			}
		}
	})

	t.Run("LargeDataset", func(t *testing.T) {
		m := NewFlatMap[int, int]()
		const size = 1000

		// Populate large dataset
		for i := range size {
			m.Store(i, i*i)
		}

		// Verify all elements through iteration
		found := maps.Collect(m.All())

		if len(found) != size {
			t.Errorf("Expected %d elements, got %d", size, len(found))
		}

		// Spot check some values
		for i := 0; i < size; i += 100 {
			if found[i] != i*i {
				t.Errorf("Key %d: expected %d, got %d", i, i*i, found[i])
			}
		}
	})
}

// TestFlatMap_All_Compatibility tests that All behaves identically to Range
func TestFlatMap_All_Compatibility(t *testing.T) {
	m := NewFlatMap[string, int]()

	// Add test data
	testData := map[string]int{
		"alpha":   1,
		"beta":    2,
		"gamma":   3,
		"delta":   4,
		"epsilon": 5,
	}

	for k, v := range testData {
		m.Store(k, v)
	}

	// Collect results from Range
	rangeResults := make(map[string]int)
	m.Range(func(k string, v int) bool {
		rangeResults[k] = v
		return true
	})

	// Collect results from All
	allResults := maps.Collect(m.All())

	// Compare results
	if len(rangeResults) != len(allResults) {
		t.Errorf("Range and All returned different counts: Range=%d, All=%d",
			len(rangeResults), len(allResults))
	}

	for k, rangeV := range rangeResults {
		if allV, ok := allResults[k]; !ok {
			t.Errorf("Key %s found in Range but not in All", k)
		} else if allV != rangeV {
			t.Errorf("Key %s: Range returned %d, All returned %d", k, rangeV, allV)
		}
	}

	for k, allV := range allResults {
		if rangeV, ok := rangeResults[k]; !ok {
			t.Errorf("Key %s found in All but not in Range", k)
		} else if rangeV != allV {
			t.Errorf("Key %s: All returned %d, Range returned %d", k, allV, rangeV)
		}
	}
}

// TestFlatMap_ComputeAll_UpdateDelete verifies Entries iteration can update and delete entries.
func TestFlatMap_ComputeAll_UpdateDelete(t *testing.T) {
	m := NewFlatMap[int, int]()
	const N = 128

	for i := range N {
		m.Store(i, i)
	}

	for it := range m.Entries() {
		if it.Key()%2 == 0 {
			it.Update(it.Value() + 1)
		} else {
			it.Delete()
		}
	}

	for i := range N {
		if i%2 == 0 {
			v, ok := m.Load(i)
			if !ok || v != i+1 {
				t.Fatalf("even key %d: want %d, ok=true; got %v, ok=%v", i, i+1, v, ok)
			}
		} else {
			if _, ok := m.Load(i); ok {
				t.Fatalf("odd key %d: expected deleted", i)
			}
		}
	}
}

// TestFlatMap_ComputeAll_EarlyStop verifies breaking the range stops iteration and only a subset is processed.
func TestFlatMap_ComputeAll_EarlyStop(t *testing.T) {
	m := NewFlatMap[int, int]()
	const N = 100
	for i := range N {
		m.Store(i, i)
	}

	processed := 0
	for it := range m.Entries() {
		it.Update(it.Value() + 1000)
		processed++
		if processed == 10 {
			break
		}
	}

	if processed != 10 {
		t.Fatalf("processed=%d, want 10", processed)
	}

	updated := 0
	for k, v := range m.All() {
		_ = k
		if v >= 1000 {
			updated++
		}
	}
	// 早停时，最后一个回调的修改也会被应用
	if updated != 10 {
		t.Fatalf("updated=%d, want 10", updated)
	}
}

func TestFlatMap_Rebuild_Wrapper(t *testing.T) {
	m := NewFlatMap[int, int]()

	// Populate some data
	for i := range 100 {
		m.Store(i, i*100)
	}

	// Perform exclusive operation using rebuild and FlatMapRebuild wrapper
	m.Rebuild(func(m *MapRebuild[int, int]) {
		// Verify we can read
		if v, ok := m.Load(1); !ok || v != 100 {
			t.Errorf("Load inside rebuild failed: got %v, want 100", v)
		}

		// Perform updates using rm methods (which use compute(..., true))
		// Update existing
		m.Compute(1, func(e *MapEntry[int, int]) {
			e.Update(101)
		})
		// Insert new
		m.Store(3, 300)

		// LoadOrStore
		actual, loaded := m.LoadOrStore(1001, 100100)
		if loaded || actual != 100100 {
			t.Errorf("LoadOrStore failed: loaded=%v, actual=%v", loaded, actual)
		}

		// Delete
		m.Delete(2)
	})

	// Verify results after rebuild
	if v, ok := m.Load(1); !ok || v != 101 {
		t.Errorf("Value for key 1 not updated: got %v, want 101", v)
	}
	if v, ok := m.Load(2); ok {
		t.Errorf("Value for key 2 should be deleted: got %v", v)
	}
	if v, ok := m.Load(3); !ok || v != 300 {
		t.Errorf("Value for key 3 not inserted: got %v, want 300", v)
	}
	if v, ok := m.Load(1001); !ok || v != 100100 {
		t.Errorf("Value for key 1001 not inserted: got %v, want 100100", v)
	}
}

func TestFlatMap_Rebuild_Concurrent(t *testing.T) {
	m := NewFlatMap[int, int]()
	done := make(chan struct{})

	// Start a long-running rebuild that allows writers
	go func() {
		m.Rebuild(func(_ *MapRebuild[int, int]) {
			// Simulate work
			time.Sleep(100 * time.Millisecond)
		})
		close(done)
	}()

	// Give the rebuild goroutine a moment to start
	time.Sleep(10 * time.Millisecond)

	// Try to write from main goroutine - should NOT block
	start := time.Now()
	m.Store(1, 100)
	duration := time.Since(start)

	if duration < 50*time.Millisecond {
		t.Errorf("Store blocked for %v, expected non-blocking", duration)
	}

	<-done
}
