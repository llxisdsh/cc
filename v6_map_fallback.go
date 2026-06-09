//go:build race

package cc

type V6Map[K comparable, V any] = Map[K, V]

func NewV6Map[K comparable, V any](options ...func(*MapConfig)) *V6Map[K, V] {
	return NewMap[K, V](options...)
}
