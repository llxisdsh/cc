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

// ------------------------------------------------------

func BenchmarkStore_cc_FunnelMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFunnelMap[int, int]()
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

func BenchmarkLoadOrStore_cc_FunnelMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFunnelMap[int, int]()
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

func BenchmarkLoad_cc_FunnelMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFunnelMap[int, int]()

	for i := range countLoad {
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

func BenchmarkDelete_cc_FunnelMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFunnelMap[int, int]()
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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

	for i := range countLoad {
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

func BenchmarkDelete_cc_Map(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewMap[int, int]()
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
				i = 0
			}
		}
	})
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
	for i := range countLoad {
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

func BenchmarkDelete_cc_FlatMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFlatMap[int, int]()
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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
	for i := range countLoad {
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

func BenchmarkDelete_xsync_Map(b *testing.B) {
	b.ReportAllocs()
	m := xsync.NewMap[int, int]()
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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
	for i := range countLoad {
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

func BenchmarkDelete_RWLockShardedMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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
	for i := range countLoad {
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

func BenchmarkDelete_original_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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
	for i := range countLoad {
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

func BenchmarkDelete_alphadose_haxmap(b *testing.B) {
	b.ReportAllocs()
	m := haxmap.New[int, int]()
	for i := range countLoad {
		m.Set(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Del(i)
			i++
			if i >= countLoad {
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
	for i := range countLoad {
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

func BenchmarkDelete_zhangyunhao116_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := skipmap.New[int, int]()
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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
// func BenchmarkDelete_riraccuia_ash(b *testing.B) {
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
	for i := range countLoad {
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

func BenchmarkDelete_fufuok_cmap(b *testing.B) {
	b.ReportAllocs()
	m := cmap.NewOf[int, int]()
	for i := range countLoad {
		m.Set(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Remove(i)
			i++
			if i >= countLoad {
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
				m.Store(i, i) // no LoadOrStore
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
	for i := range countLoad {
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

func BenchmarkDelete_mhmtszr_concurrent_swiss_map(b *testing.B) {
	b.ReportAllocs()
	m := csmap.New(csmap.WithShardCount[int, int](32))
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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
	for i := range countLoad {
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

func BenchmarkDelete_orcaman_concurrent_map(b *testing.B) {
	b.ReportAllocs()
	m := orcaman_map.NewWithCustomShardingFunction[int, int](
		func(key int) uint32 {
			return uint32(key)
		},
	)
	for i := range countLoad {
		m.Set(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Remove(i)
			i++
			if i >= countLoad {
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
	for i := range countLoad {
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

func BenchmarkDelete_snawoot_lfmap(b *testing.B) {
	b.ReportAllocs()
	m := lfmap.New[int, int]()
	for i := range countLoad {
		m.Set(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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
	for i := range countLoad {
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

func BenchmarkDelete_RWLockMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockMap[int, int]()
	for i := range countLoad {
		m.Store(i, i)
	}
	runtime.GC()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Delete(i)
			i++
			if i >= countLoad {
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

	var i int
	for b.Loop() {
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

	var i int
	for b.Loop() {
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
	for i := range countLoad {
		m[i] = i
	}
	runtime.GC()

	var i int
	for b.Loop() {
		_, _ = m[i]
		i++
		if i >= countLoad {
			i = 0
		}
	}
}

func BenchmarkDelete_stdMap(b *testing.B) {
	b.ReportAllocs()
	m := make(map[int]int)
	for i := range countLoad {
		m[i] = i
	}
	runtime.GC()

	var i int
	for b.Loop() {
		i++
		delete(m, i)
		if i >= countLoad {
			i = 0
		}
	}
}

// ------------------------------------------------------

// RWLockMap is a generic thread-safe key-value store.
type RWLockMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// NewRWLockMap creates a new RWLockMap instance.
func NewRWLockMap[K comparable, V any]() *RWLockMap[K, V] {
	return &RWLockMap[K, V]{
		m: make(map[K]V),
	}
}

// Load returns the value for the key, or zero value if key does not exist.
func (gm *RWLockMap[K, V]) Load(key K) (V, bool) {
	gm.mu.RLock()
	v, ok := gm.m[key]
	gm.mu.RUnlock()
	return v, ok
}

// Store stores the value for the specified key.
func (gm *RWLockMap[K, V]) Store(key K, value V) {
	gm.mu.Lock()
	gm.m[key] = value
	gm.mu.Unlock()
}

// LoadOrStore stores the value if the key does not exist and returns false; if the key exists, returns the existing value and true.
func (gm *RWLockMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if v, ok := gm.m[key]; ok {
		return v, true
	}
	gm.m[key] = value
	return value, false
}

// Delete deletes the value for the specified key.
func (gm *RWLockMap[K, V]) Delete(key K) {
	gm.mu.Lock()
	delete(gm.m, key)
	gm.mu.Unlock()
}

// Range iterates over all key-value pairs and executes the specified function.
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

// RWLockShardedMap is a generic thread-safe key-value store using sharded locks (RWMutex).
type RWLockShardedMap[K comparable, V any] struct {
	shards    []shard[K, V] // shard array
	shardMask uintptr       // shard count
	hashFunc  cc.HashFunc
	seed      uintptr
}

// shard is the internal structure for each shard, containing an RWMutex and a regular map.
type shard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// NewRWLockShardedMap creates a new RWLockShardedMap instance.
func NewRWLockShardedMap[K comparable, V any](
	shardCnt int,
) *RWLockShardedMap[K, V] {
	if shardCnt <= 0 {
		shardCnt = 1 // default to at least one shard
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

// shardIndex calculates the shard index based on the hash of the key.
func (sm *RWLockShardedMap[K, V]) shardIndex(key K) uintptr {
	return (sm.shardMask - 1) & sm.hashFunc(noescape(unsafe.Pointer(&key)), 0)
}

// Load returns the value for the key, or zero value if key does not exist.
func (sm *RWLockShardedMap[K, V]) Load(key K) (V, bool) {
	shard := &sm.shards[sm.shardIndex(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	val, ok := shard.m[key]
	return val, ok
}

// Store stores the value for the specified key.
func (sm *RWLockShardedMap[K, V]) Store(key K, value V) {
	shard := &sm.shards[sm.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.m[key] = value
}

// LoadOrStore stores the value if the key does not exist and returns false; if the key exists, returns the existing value and true.
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

// Delete deletes the value for the specified key.
func (sm *RWLockShardedMap[K, V]) Delete(key K) {
	shard := &sm.shards[sm.shardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.m, key)
}

// Range iterates over all key-value pairs and executes the specified function.
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
