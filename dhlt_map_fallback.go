//go:build race || (!amd64 && !arm64)

package cc

type DHLTMap[K comparable, V any] = Map[K, V]

func NewDHLTMap[K comparable, V any](options ...func(*MapConfig)) *DHLTMap[K, V] {
	return NewMap[K, V](options...)
}
