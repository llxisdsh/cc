package benchmark

import (
	"sync"

	"github.com/alphadose/haxmap"
	"github.com/jeremiah-masters/dlht"
	csmap "github.com/mhmtszr/concurrent-swiss-map"
)

type stableFactory[K comparable] struct {
	name string
	new  func(capHint int) stableMapOps[K]
}

type stableMapOps[K comparable] struct {
	insert func(K, int)
	load   func(K) (int, bool)
	delete func(K)
	size   func() int
}

func (m stableMapOps[K]) Insert(k K, v int)    { m.insert(k, v) }
func (m stableMapOps[K]) Load(k K) (int, bool) { return m.load(k) }
func (m stableMapOps[K]) Delete(k K)           { m.delete(k) }
func (m stableMapOps[K]) Size() int            { return m.size() }

type stableLoadOrStoreMap[K comparable] interface {
	LoadOrStore(K, int) (int, bool)
	Load(K) (int, bool)
	Delete(K)
	Size() int
}

type stableHaxKey interface {
	~int | ~string
}

func stableLoadOrStoreFactory[K comparable, M stableLoadOrStoreMap[K]](
	name string,
	newMap func(capHint int) M,
) stableFactory[K] {
	return stableFactory[K]{
		name: name,
		new: func(capHint int) stableMapOps[K] {
			m := newMap(capHint)
			return stableMapOps[K]{
				insert: func(k K, v int) { m.LoadOrStore(k, v) },
				load:   m.Load,
				delete: m.Delete,
				size:   m.Size,
			}
		},
	}
}

func stableDLHTFactory[K comparable](name string) stableFactory[K] {
	return stableFactory[K]{
		name: name,
		new: func(capHint int) stableMapOps[K] {
			m := dlht.New[K, int](dlht.Options{InitialSize: uint64(capHint)})
			return stableMapOps[K]{
				insert: func(k K, v int) { m.Insert(k, v) },
				load:   m.Get,
				delete: func(k K) { m.Delete(k) },
				size:   func() int { return int(m.Size()) },
			}
		},
	}
}

func stableSyncMapFactory[K comparable](name string) stableFactory[K] {
	return stableFactory[K]{
		name: name,
		new: func(capHint int) stableMapOps[K] {
			m := &sync.Map{}
			return stableMapOps[K]{
				insert: func(k K, v int) { m.LoadOrStore(k, v) },
				load: func(k K) (int, bool) {
					v, ok := m.Load(k)
					if !ok {
						return 0, false
					}
					return v.(int), true
				},
				delete: func(k K) { m.Delete(k) },
				size: func() int {
					size := 0
					m.Range(func(key, value any) bool {
						size++
						return true
					})
					return size
				},
			}
		},
	}
}

func stableHaxFactory[K stableHaxKey](name string) stableFactory[K] {
	return stableFactory[K]{
		name: name,
		new: func(capHint int) stableMapOps[K] {
			m := haxmap.New[K, int](uintptr(capHint))
			return stableMapOps[K]{
				insert: func(k K, v int) { m.GetOrSet(k, v) },
				load:   m.Get,
				delete: func(k K) { m.Del(k) },
				size:   func() int { return int(m.Len()) },
			}
		},
	}
}

func stableCSMapFactory[K comparable](name string) stableFactory[K] {
	return stableFactory[K]{
		name: name,
		new: func(capHint int) stableMapOps[K] {
			m := csmap.New[K, int](csmap.WithSize[K, int](uint64(capHint)))
			return stableMapOps[K]{
				insert: func(k K, v int) { m.SetIfAbsent(k, v) },
				load:   m.Load,
				delete: func(k K) { m.Delete(k) },
				size:   m.Count,
			}
		},
	}
}
