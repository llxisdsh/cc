package cc

import "github.com/llxisdsh/cc/internal/opt"

// Entry is a temporary view of a map entry
// It can be updated or deleted during the callback.
//
// WARNING:
// - Only valid inside the callback; do NOT keep, return, or use it outside.
// - Not safe across goroutines.
// 警告：仅在回调期间有效；不可保存或让其指针逃逸，也不可跨协程使用。
type Entry[K comparable, V any] struct {
	entry  entry_[K, V]
	loaded bool
	op     computeOp
}

// Key returns the entry's key.
//
//go:nosplit
func (e *Entry[K, V]) Key() K {
	return e.entry.Key
}

// Value returns the entry's value. Returns zero value if not loaded.
//
//go:nosplit
func (e *Entry[K, V]) Value() V {
	return e.entry.Value
}

// Loaded reports whether the entry exists in the map.
//
//go:nosplit
func (e *Entry[K, V]) Loaded() bool {
	return e.loaded
}

// Update sets the entry's value. Inserts it if not loaded, replaces if loaded.
//
//go:nosplit
func (e *Entry[K, V]) Update(value V) {
	e.entry.Value = value
	e.op = updateOp
}

// Delete marks the entry for removal and clears its value.
//
//go:nosplit
func (e *Entry[K, V]) Delete() {
	e.entry.Value = *new(V)
	e.op = deleteOp
}

// entry_ is the internal representation of a map entry.
type entry_[K comparable, V any] struct {
	opt.EmbeddedHash
	Key   K
	Value V
}
