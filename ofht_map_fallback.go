//go:build race

package cc

type OFHTMap[K comparable, V any] = Map[K, V]

func NewOFHTMap[K comparable, V any](options ...func(*MapConfig)) *OFHTMap[K, V] {
	return NewMap[K, V](options...)
}
