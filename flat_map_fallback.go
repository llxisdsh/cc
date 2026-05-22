//go:build race

package cc

type FlatMap[K comparable, V any] = Map[K, V]

func NewFlatMap[K comparable, V any](options ...func(*MapConfig)) *FlatMap[K, V] {
	return NewMap[K, V](options...)
}
