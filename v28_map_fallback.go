//go:build race || !amd64 || !goexperiment.simd

package cc

type V28Map[K comparable, V any] = Map[K, V]

func NewV28Map[K comparable, V any](options ...func(*MapConfig)) *V28Map[K, V] {
	return NewMap[K, V](options...)
}
