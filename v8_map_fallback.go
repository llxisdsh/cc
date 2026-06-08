//go:build race

package cc

type V8Map[K comparable, V any] = Map[K, V]

func NewV8Map[K comparable, V any](options ...func(*MapConfig)) *V8Map[K, V] {
	return NewMap[K, V](options...)
}
