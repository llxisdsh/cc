package benchmark

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	// "github.com/riraccuia/ash"

	"github.com/Snawoot/lfmap"
	"github.com/alphadose/haxmap"
	"github.com/fufuok/cmap"
	"github.com/llxisdsh/cc"
	csmap "github.com/mhmtszr/concurrent-swiss-map"
	orcaman_map "github.com/orcaman/concurrent-map/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/zhangyunhao116/skipmap"
)

const (
	countStore       = 1_000_000
	countLoadOrStore = countStore
	countLoad        = min(1_000_000, countStore)
)

func mixRand(i int) int {
	return i & (8 - 1)
	// return (key * 11400714819323198485) >> 32 // 2^64/φ
}

// ------------------------------------------------------

func BenchmarkStore_cc_FlatMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFlatMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_cc_FlatMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFlatMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_cc_FlatMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFlatMap[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_cc_FlatMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFlatMap[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Store(i, i)
			case 1:
				m.Delete(i)
			case 2:
				_, _ = m.LoadOrStore(i, i)
			default:
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// ------------------------------------------------------

func BenchmarkStore_cc_Map(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_cc_Map(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_cc_Map(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewMap[int, int]()

	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_cc_Map(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewMap[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Store(i, i)
			case 1:
				m.Delete(i)
			case 2:
				_, _ = m.LoadOrStore(i, i)
			default:
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// --------------------------------------------------------------

func BenchmarkStore_xsync_Map(b *testing.B) {
	b.ReportAllocs()
	m := xsync.NewMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_xsync_Map(b *testing.B) {
	b.ReportAllocs()
	m := xsync.NewMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_xsync_Map(b *testing.B) {
	b.ReportAllocs()
	m := xsync.NewMap[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_xsync_Map(b *testing.B) {
	b.ReportAllocs()
	m := xsync.NewMap[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Store(i, i)
			case 1:
				m.Delete(i)
			case 2:
				_, _ = m.LoadOrStore(i, i)
			default:
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// --------------------------------------------------------------

func BenchmarkStore_RWLockShardedMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_RWLockShardedMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_RWLockShardedMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_RWLockShardedMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Store(i, i)
			case 1:
				m.Delete(i)
			case 2:
				_, _ = m.LoadOrStore(i, i)
			default:
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// ------------------------------------------------------
func BenchmarkStore_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Store(i, i)
			case 1:
				m.Delete(i)
			case 2:
				_, _ = m.LoadOrStore(i, i)
			default:
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// ------------------------------------------------------

func BenchmarkStore_alphadose_haxmap(b *testing.B) {
	b.ReportAllocs()
	m := haxmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_alphadose_haxmap(b *testing.B) {
	b.ReportAllocs()
	m := haxmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.GetOrSet(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_alphadose_haxmap(b *testing.B) {
	b.ReportAllocs()
	m := haxmap.New[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Set(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Get(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_alphadose_haxmap(b *testing.B) {
	b.ReportAllocs()
	m := haxmap.New[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Set(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Set(i, i)
			case 1:
				m.Del(i)
			case 2:
				_, _ = m.GetOrSet(i, i)
			default:
				_, _ = m.Get(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// ------------------------------------------------------

func BenchmarkStore_zhangyunhao116_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := skipmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_zhangyunhao116_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := skipmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_zhangyunhao116_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := skipmap.New[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_zhangyunhao116_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := skipmap.New[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Store(i, i)
			case 1:
				m.Delete(i)
			case 2:
				_, _ = m.LoadOrStore(i, i)
			default:
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

//
// // --------------------------------------------------------------
// func BenchmarkStore_riraccuia_ash(b *testing.B) {
// 	b.ReportAllocs()
// 	m := new(ash.Map).From(ash.NewSkipList(32))
// 	runtime.GC()
// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		i := 0
// 		for pb.Next() {
// 			m.Store(i, i)
// 			i++
// 			if i >= countStore {
// 				i = 0
// 			}
// 		}
// 	})
// }
//
// func BenchmarkLoadOrStore_riraccuia_ash(b *testing.B) {
// 	b.ReportAllocs()
// 	m := new(ash.Map).From(ash.NewSkipList(32))
// 	runtime.GC()
// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		i := 0
// 		for pb.Next() {
// 			_, _ = m.LoadOrStore(i, i)
// 			i++
// 			if i >= countLoadOrStore {
// 				i = 0
// 			}
// 		}
// 	})
// }
//
// func BenchmarkLoad_riraccuia_ash(b *testing.B) {
// 	b.ReportAllocs()
// 	m := new(ash.Map).From(ash.NewSkipList(32))
// 	for i := 0; i < countLoad; i++ {
// 		m.Store(i, i)
// 	}
// 	runtime.GC()
// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		i := 0
// 		for pb.Next() {
// 			_, _ = m.Load(i)
// 			i++
// 			if i >= countLoad {
// 				i = 0
// 			}
// 		}
// 	})
// }
//
// func BenchmarkMixed_riraccuia_ash(b *testing.B) {
// 	b.ReportAllocs()
// 	m := new(ash.Map).From(ash.NewSkipList(32))
// 	for i := 0; i < countLoad; i++ {
// 		m.Store(i, i)
// 	}
// 	runtime.GC()
//
// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		i := 0
// 		for pb.Next() {
// 			switch mixRand(i) {
// 			case 0:
// 				m.Store(i, i)
// 			case 1:
// 				m.Delete(i)
// 			case 2:
// 				_, _ = m.LoadOrStore(i, i)
// 			default:
// 				_, _ = m.Load(i)
// 			}
// 			i++
// 			if i >= countLoad<<1 {
// 				i = 0
// 			}
// 		}
// 	})
// }

// ------------------------------------------------------
func BenchmarkStore_fufuok_cmap(b *testing.B) {
	b.ReportAllocs()
	m := cmap.NewOf[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_fufuok_cmap(b *testing.B) {
	b.ReportAllocs()
	m := cmap.NewOf[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = m.SetIfAbsent(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_fufuok_cmap(b *testing.B) {
	b.ReportAllocs()
	m := cmap.NewOf[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Set(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Get(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_fufuok_cmap(b *testing.B) {
	b.ReportAllocs()
	m := cmap.NewOf[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Set(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Set(i, i)
			case 1:
				m.Remove(i)
			case 2:
				_ = m.SetIfAbsent(i, i)
			default:
				_, _ = m.Get(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// ------------------------------------------------------

func BenchmarkStore_mhmtszr_concurrent_swiss_map(b *testing.B) {
	b.ReportAllocs()
	m := csmap.New(csmap.WithShardCount[int, int](32))
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_mhmtszr_concurrent_swiss_map(b *testing.B) {
	b.ReportAllocs()
	m := csmap.New(csmap.WithShardCount[int, int](32))
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if !m.Has(i) {
				m.Store(i, i) // 没有LoadOrStore
			}
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_mhmtszr_concurrent_swiss_map(b *testing.B) {
	b.ReportAllocs()
	m := csmap.New(csmap.WithShardCount[int, int](32))
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_mhmtszr_concurrent_swiss_map(b *testing.B) {
	b.ReportAllocs()
	m := csmap.New(csmap.WithShardCount[int, int](32))
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Store(i, i)
			case 1:
				m.Delete(i)
			case 2:
				m.SetIfAbsent(i, i)
			default:
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// // --------------------------------------------------------------
// set so slow
// func BenchmarkStore_cornelk_hashmap(b *testing.B) {
//	b.ReportAllocs()
//	var m = hashmap.New[int, int]()
//	runtime.GC()
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		i := 0
//		for pb.Next() {
//			m.Set(i, i)
//			i++
//			if i >= countStore {
//				i = 0
//			}
//		}
//	})
// }
//
// func BenchmarkLoadOrStore_cornelk_hashmap(b *testing.B) {
//	b.ReportAllocs()
//	var m = hashmap.New[int, int]()
//	runtime.GC()
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		i := 0
//		for pb.Next() {
//			_, _ = m.GetOrInsert(i, i)
//			i++
//			if i >= countLoadOrStore {
//				i = 0
//			}
//		}
//	})
// }
//
// func BenchmarkLoad_cornelk_hashmap(b *testing.B) {
//	b.ReportAllocs()
//	var m = hashmap.New[int, int]()
//	for i := 0; i < countLoad; i++ {
//		m.Set(i, i)
//	}
//	runtime.GC()
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		i := 0
//		for pb.Next() {
//			_, _ = m.Get(i)
//			i++
//			if i >= countLoad {
//				i = 0
//			}
//		}
//	})
// }

// --------------------------------------------------------------

func BenchmarkStore_orcaman_concurrent_map(b *testing.B) {
	b.ReportAllocs()
	m := orcaman_map.NewWithCustomShardingFunction[int, int](
		func(key int) uint32 {
			return uint32(key)
		},
	)
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_orcaman_concurrent_map(b *testing.B) {
	b.ReportAllocs()
	m := orcaman_map.NewWithCustomShardingFunction[int, int](
		func(key int) uint32 {
			return uint32(key)
		},
	)
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = m.SetIfAbsent(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_orcaman_concurrent_map(b *testing.B) {
	b.ReportAllocs()
	m := orcaman_map.NewWithCustomShardingFunction[int, int](
		func(key int) uint32 {
			return uint32(key)
		},
	)
	for i := 0; i < countLoad; i++ {
		m.Set(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Get(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_orcaman_concurrent_map(b *testing.B) {
	b.ReportAllocs()
	m := orcaman_map.NewWithCustomShardingFunction[int, int](
		func(key int) uint32 {
			return uint32(key)
		},
	)
	for i := 0; i < countLoad; i++ {
		m.Set(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Set(i, i)
			case 1:
				m.Remove(i)
			case 2:
				m.SetIfAbsent(i, i)
			default:
				_, _ = m.Get(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// --------------------------------------------------------------
// need : --tags=safety_map
// func BenchmarkStore_realfox_order_map(b *testing.B) {
//	b.ReportAllocs()
//	m := odmap.New[int, int]()
//	runtime.GC()
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		i := 0
//		for pb.Next() {
//			m.Store(i, i)
//			i++
//			if i >= countStore {
//				i = 0
//			}
//		}
//	})
// }
//
// func BenchmarkLoadOrStore_realfox_order_map(b *testing.B) {
//	b.ReportAllocs()
//	m := odmap.New[int, int]()
//	runtime.GC()
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		i := 0
//		for pb.Next() {
//			_, _ = m.LoadOrStore(i, i)
//			i++
//			if i >= countLoadOrStore {
//				i = 0
//			}
//		}
//	})
// }
//
// func BenchmarkLoad_realfox_order_map(b *testing.B) {
//	b.ReportAllocs()
//	m := odmap.New[int, int]()
//	for i := 0; i < countLoad; i++ {
//		m.Store(i, i)
//	}
//	runtime.GC()
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		i := 0
//		for pb.Next() {
//			_, _ = m.Load(i)
//			i++
//			if i >= countLoad {
//				i = 0
//			}
//		}
//	})
// }

// --------------------------------------------------------------
func BenchmarkStore_snawoot_lfmap(b *testing.B) {
	b.ReportAllocs()
	m := lfmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Set(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_snawoot_lfmap(b *testing.B) {
	b.ReportAllocs()
	m := lfmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := m.Get(i)
			if !ok {
				m.Set(i, i)
			}
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_snawoot_lfmap(b *testing.B) {
	b.ReportAllocs()
	m := lfmap.New[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Set(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Get(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_snawoot_lfmap(b *testing.B) {
	b.ReportAllocs()
	m := lfmap.New[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Set(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Set(i, i)
			case 1:
				m.Delete(i)
			case 2:
				_, ok := m.Get(i)
				if !ok {
					m.Set(i, i)
				}
			default:
				_, _ = m.Get(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// --------------------------------------------------------------

func BenchmarkStore_RWLockMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Store(i, i)
			i++
			if i >= countStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoadOrStore_RWLockMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.LoadOrStore(i, i)
			i++
			if i >= countLoadOrStore {
				i = 0
			}
		}
	})
}

func BenchmarkLoad_RWLockMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockMap[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Load(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
}

func BenchmarkMixed_RWLockMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockMap[int, int]()
	for i := 0; i < countLoad; i++ {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch mixRand(i) {
			case 0:
				m.Store(i, i)
			case 1:
				m.Delete(i)
			case 2:
				_, _ = m.LoadOrStore(i, i)
			default:
				_, _ = m.Load(i)
			}
			i++
			if i >= countLoad<<1 {
				i = 0
			}
		}
	})
}

// ------------------------------------------------------
func BenchmarkStore_stdMap(b *testing.B) {
	b.ReportAllocs()
	m := make(map[int]int)
	runtime.GC()
	b.ResetTimer()
	var i int
	for range b.N {
		m[i] = i
		i++
		if i >= countStore {
			i = 0
		}
	}
}

func BenchmarkLoadOrStore_stdMap(b *testing.B) {
	b.ReportAllocs()
	m := make(map[int]int)
	runtime.GC()
	b.ResetTimer()
	var i int
	for range b.N {
		if _, ok := m[i]; !ok {
			m[i] = i
		}

		i++
		if i >= countStore {
			i = 0
		}
	}
}

func BenchmarkLoad_stdMap(b *testing.B) {
	b.ReportAllocs()
	m := make(map[int]int)
	for i := 0; i < countLoad; i++ {
		m[i] = i
	}
	runtime.GC()
	b.ResetTimer()
	var i int
	for range b.N {
		_, _ = m[i]
		i++
		if i >= countLoad {
			i = 0
		}
	}
}

func BenchmarkMixed_stdMap(b *testing.B) {
	b.ReportAllocs()
	m := make(map[int]int)
	for i := 0; i < countLoad; i++ {
		m[i] = i
	}
	runtime.GC()

	b.ResetTimer()

	var i int
	for range b.N {
		switch mixRand(i) {
		case 0:
			m[i] = i
		case 1:
			delete(m, i)
		case 2:
			if _, ok := m[i]; !ok {
				m[i] = i
			}
		default:
			_, _ = m[i]
		}
		i++
		if i >= countLoad<<1 {
			i = 0
		}
	}
}

// ------------------------------------------------------

// RWLockMap 是一个泛型化的线程安全的键值存储。
type RWLockMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// NewGenericMap 创建一个新的 RWLockMap 实例。
func NewRWLockMap[K comparable, V any]() *RWLockMap[K, V] {
	return &RWLockMap[K, V]{
		m: make(map[K]V),
	}
}

// Load 返回键对应的值，如果键不存在则返回零值。
func (gm *RWLockMap[K, V]) Load(key K) (V, bool) {
	gm.mu.RLock()
	v, ok := gm.m[key]
	gm.mu.RUnlock()
	return v, ok
}

// Store 将值存储到指定的键中。
func (gm *RWLockMap[K, V]) Store(key K, value V) {
	gm.mu.Lock()
	gm.m[key] = value
	gm.mu.Unlock()
}

// LoadOrStore 如果键不存在，则存储值并返回 false；如果键已存在，则返回现有值和 true。
func (gm *RWLockMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if v, ok := gm.m[key]; ok {
		return v, true
	}
	gm.m[key] = value
	return value, false
}

// Delete 删除指定键的值。
func (gm *RWLockMap[K, V]) Delete(key K) {
	gm.mu.Lock()
	delete(gm.m, key)
	gm.mu.Unlock()
}

// Range 遍历所有键值对，执行指定的函数。
func (gm *RWLockMap[K, V]) Range(f func(K, V) bool) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	for k, v := range gm.m {
		if !f(k, v) {
			break
		}
	}
}

// ------------------------------------------------------

// RWLockShardedMap 是一个使用分段锁（RWMutex）的泛型化线程安全的键值存储。
type RWLockShardedMap[K comparable, V any] struct {
	shards    []shard[K, V] // 分段锁数组
	shardMask uintptr       // 分段数量
	hashFunc  cc.HashFunc
	seed      uintptr
}

// shard 是每个分段的内部结构，包含一个 RWMutex 和一个普通 map。
type shard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// NewRWLockShardedMap 创建一个新的 RWLockShardedMap 实例。
func NewRWLockShardedMap[K comparable, V any](
	shardCnt int,
) *RWLockShardedMap[K, V] {
	if shardCnt <= 0 {
		shardCnt = 1 // 默认至少有一个分段
	}
	shardCnt = nextPowOf2(shardCnt)
	shards := make([]shard[K, V], shardCnt)
	for i := range shards {
		shards[i] = shard[K, V]{m: make(map[K]V)}
	}
	return &RWLockShardedMap[K, V]{
		shards:    shards,
		shardMask: uintptr(shardCnt) - 1,
		hashFunc:  cc.GetBuiltInHasher[K](),
		seed:      uintptr(rand.Uint64()),
	}
}

//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

func nextPowOf2(n int) int {
	if n <= 0 {
		return 1
	}
	v := n - 1
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	if bits.UintSize >= 64 {
		v |= v >> 32
	}
	return v + 1
}

// shardIndex 根据键的哈希值计算分段索引。
func (sm *RWLockShardedMap[K, V]) shardIndex(key K) uintptr {
	return (sm.shardMask - 1) & sm.hashFunc(noescape(unsafe.Pointer(&key)), 0)
}

// Load 返回键对应的值，如果键不存在则返回零值。
func (sm *RWLockShardedMap[K, V]) Load(key K) (V, bool) {
	shard := &sm.shards[sm.shardIndex(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	val, ok := shard.m[key]
	return val, ok
}

// Store 将值存储到指定的键中。
func (sm *RWLockShardedMap[K, V]) Store(key K, value V) {
	shard := &sm.shards[sm.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.m[key] = value
}

// LoadOrStore 如果键不存在，则存储值并返回 false；如果键已存在，则返回现有值和 true。
func (sm *RWLockShardedMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	shard := &sm.shards[sm.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if val, ok := shard.m[key]; ok {
		return val, true
	}
	shard.m[key] = value
	return value, false
}

// Delete 删除指定键的值。
func (sm *RWLockShardedMap[K, V]) Delete(key K) {
	shard := &sm.shards[sm.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.m, key)
}

// Range 遍历所有键值对，执行指定的函数。
func (sm *RWLockShardedMap[K, V]) Range(f func(K, V) bool) {
	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.RLock()
		for k, v := range shard.m {
			if !f(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

func (sm *RWLockShardedMap[K, V]) Size() int {
	size := 0
	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.RLock()
		size += len(shard.m)
		shard.mu.RUnlock()
	}
	return size
}
