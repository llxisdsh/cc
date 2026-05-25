package cc

import (
	"unsafe"
)

// MapRebuild provides access to map operations during a rebuild.
// It wraps either a Map or a FlatMap, delegating operations to the underlying map.
// All operations on this struct ignore the rebuild hint (assuming the caller holds the rebuild lock).
//
// WARNING:
// - Only valid inside the callback; do NOT keep, return, or use it outside.
// - Not safe across goroutines.
// 警告：仅在回调期间有效；不可保存或让其指针逃逸，也不可跨协程使用。
type MapRebuild[K comparable, V any] struct {
	m *Map[K, V]
	f *FlatMap[K, V]
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *MapRebuild[K, V]) Load(key K) (value V, ok bool) {
	if m.m != nil {
		return m.m.Load(key)
	}
	return m.f.Load(key)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *MapRebuild[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return m.Compute(key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(value)
	})
}

// LoadOrStoreFn loads the value for a key if present.
// Otherwise, it stores and returns the value returned by valueFn.
// The loaded result is true if the value was loaded, false if stored.
func (m *MapRebuild[K, V]) LoadOrStoreFn(key K, valueFn func() V) (actual V, loaded bool) {
	return m.Compute(key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			return
		}
		e.Update(valueFn())
	})
}

// LoadAndUpdate updates the value for key if it exists, returning the previous
// value. The loaded result reports whether the key was present.
func (m *MapRebuild[K, V]) LoadAndUpdate(key K, value V) (previous V, loaded bool) {
	_, loaded = m.Compute(key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			previous = e.Value()
			e.Update(value)
		}
	})
	return previous, loaded
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (m *MapRebuild[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	_, loaded = m.Compute(key, func(e *MapEntry[K, V]) {
		if e.Loaded() {
			previous = e.Value()
			e.Delete()
		}
	})
	return previous, loaded
}

// Store sets the value for a key.
func (m *MapRebuild[K, V]) Store(key K, value V) {
	m.Compute(key, func(e *MapEntry[K, V]) {
		e.Update(value)
	})
}

// Swap swaps the value for a key and returns the previous value if any.
// The loaded result reports whether the key was present.
func (m *MapRebuild[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	_, loaded = m.Compute(key, func(e *MapEntry[K, V]) {
		previous = e.Value()
		e.Update(value)
	})
	return previous, loaded
}

// Delete deletes the value for a key.
func (m *MapRebuild[K, V]) Delete(key K) {
	m.Compute(key, func(e *MapEntry[K, V]) {
		e.Delete()
	})
}

// Compute executes a function for a specific key.
// It is safe to call inside a rebuild callback.
func (m *MapRebuild[K, V]) Compute(
	key K,
	fn func(e *MapEntry[K, V]),
) (actual V, loaded bool) {
	if m.m != nil {
		return m.m.compute(&key, unsafe.Pointer(&fn), computeInit|computeIgnoreHint)
	}
	return m.f.compute(&key, unsafe.Pointer(&fn), computeInit|computeIgnoreHint)
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
func (m *MapRebuild[K, V]) Range(yield func(key K, value V) bool) {
	if m.m != nil {
		m.m.Range(yield)
		return
	}
	m.f.Range(yield)
}

// All returns an iterator function for use with range-over-func.
// It provides the same functionality as Range but in iterator form.
func (m *MapRebuild[K, V]) All() func(yield func(K, V) bool) {
	return m.Range
}

// ComputeRange iterates all entries and applies a user callback.
// If f returns false, range stops the iteration.
func (m *MapRebuild[K, V]) ComputeRange(yield func(e *MapEntry[K, V]) bool) {
	if m.m != nil {
		m.m.computeRange(yield, true)
		return
	}
	m.f.computeRange(yield, true)
}

// Entries returns an iterator function for use with range-over-func.
// It provides the same functionality as ComputeRange but in iterator form.
func (m *MapRebuild[K, V]) Entries() func(yield func(e *MapEntry[K, V]) bool) {
	return m.ComputeRange
}

// Size returns the number of key-value pairs in the map.
// This operation sums counters across all size stripes for an approximate
// count.
func (m *MapRebuild[K, V]) Size() int {
	if m.m != nil {
		return m.m.Size()
	}
	return m.f.Size()
}

// ToMap collect up to limit entries into a map[K]V, limit < 0 is no limit.
func (m *MapRebuild[K, V]) ToMap(limit ...int) map[K]V {
	if m.m != nil {
		return m.m.ToMap(limit...)
	}
	return m.f.ToMap(limit...)
}
