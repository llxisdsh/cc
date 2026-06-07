//go:build race

package cc

type V4Map[K comparable, V any] = Map[K, V]

func NewV4Map[K comparable, V any](options ...func(*MapConfig)) *V4Map[K, V] {
	return NewMap[K, V](options...)
}
