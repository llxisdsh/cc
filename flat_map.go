//go:build !race

package cc

// FlatMap is the recommended flat-storage concurrent map.
//
// It keeps the stable FlatMap API while using the current compact SWAR-probed
// engine underneath. Prefer FlatMap for performance-sensitive workloads that
// benefit from low GC pressure and fast iteration.
type FlatMap[K comparable, V any] = V6Map[K, V]

func NewFlatMap[K comparable, V any](options ...func(*MapConfig)) *FlatMap[K, V] {
	return NewV6Map[K, V](options...)
}
