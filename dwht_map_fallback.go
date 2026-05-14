//go:build race || (!amd64 && !arm64)

package cc

type DWHTMap[K comparable, V any] = Map[K, V]

func NewDWHTMap[K comparable, V any](options ...func(*MapConfig)) *DWHTMap[K, V] {
	return NewMap[K, V](options...)
}
