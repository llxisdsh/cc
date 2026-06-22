package cc

// MapEntry is a temporary view of a map entry
// It can be updated or deleted during the callback.
//
// WARNING:
// - Only valid inside the callback; do NOT keep, return, or use it outside.
// - Not safe across goroutines.
// 警告：仅在回调期间有效；不可保存或让其指针逃逸，也不可跨协程使用。
type MapEntry[K comparable, V any] struct {
	entry  entryNoHash[K, V]
	loaded bool
	op     computeOp
}

// Key returns the entry's key.
//
//go:nosplit
func (e *MapEntry[K, V]) Key() K {
	return e.entry.key
}

// Value returns the entry's value. Returns zero value if not loaded.
//
//go:nosplit
func (e *MapEntry[K, V]) Value() V {
	return e.entry.val
}

// Loaded reports whether the entry exists in the map.
//
//go:nosplit
func (e *MapEntry[K, V]) Loaded() bool {
	return e.loaded
}

// Update sets the entry's value. Inserts it if not loaded, replaces if loaded.
//
//go:nosplit
func (e *MapEntry[K, V]) Update(value V) {
	e.entry.val = value
	e.op = updateOp
}

// Delete marks the entry for removal and clears its value.
//
//go:nosplit
func (e *MapEntry[K, V]) Delete() {
	e.entry.val = *new(V)
	e.op = deleteOp
}

type entryNoHash[K comparable, V any] struct {
	key K
	val V
}

type entryWithHash[K comparable, V any] struct {
	hash uintptr
	key  K
	val  V
}
