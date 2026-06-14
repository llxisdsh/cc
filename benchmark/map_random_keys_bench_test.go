package benchmark

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
	_ "unsafe"

	"github.com/alphadose/haxmap"
	"github.com/llxisdsh/cc"
	csmap "github.com/mhmtszr/concurrent-swiss-map"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/zhangyunhao116/skipmap"
)

// ============================================================================
// Objective concurrent benchmarks
// Test scenarios: int/string key, Store/Load/Delete/Range operations
// Comparison targets: cc.Map, cc.FlatMap, sync.Map, xsync.MapV4, skipmap, gg.skipmap, haxmap, RWLockShardedMap
// ============================================================================

const (
	benchKeyCount = 10_000 // pre-generated key count
)

// pre-generated random int keys
var preGenIntKeys []int

// pre-generated random string keys
var preGenStringKeys []string

func init() {
	// pre-generate random int keys
	preGenIntKeys = make([]int, benchKeyCount)
	for i := range preGenIntKeys {
		preGenIntKeys[i] = int(runtime_cheaprand())
	}

	// pre-generate random string keys
	preGenStringKeys = make([]string, benchKeyCount)
	for i := range preGenStringKeys {
		preGenStringKeys[i] = strconv.FormatInt(int64(runtime_cheaprand()), 36)
	}
}

// keyIndex is used to get the current key index in concurrent tests
func keyIndex(i int) int {
	return i % benchKeyCount
}

// ============================================================================
// Int Key - Store
// ============================================================================

func BenchmarkConcurrent_IntStore_cc_DWHTMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewDWHTMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_cc_OFHTMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewOFHTMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_cc_FunnelMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFunnelMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_ccMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_ccFlatMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFlatMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := skipmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_cc_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewSkipMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_haxmap(b *testing.B) {
	b.ReportAllocs()
	m := haxmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Set(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_xsyncMap(b *testing.B) {
	b.ReportAllocs()
	m := xsync.NewMap[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_CsMap(b *testing.B) {
	b.ReportAllocs()
	m := csmap.New[int, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntStore_RWShardedMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Store(key, key)
			i = keyIndex(i + 1)
		}
	})
}

// ============================================================================
// Int Key - Load (pre-populated data)
// ============================================================================

func BenchmarkConcurrent_IntLoad_cc_DWHTMap(b *testing.B) {
	m := cc.NewDWHTMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_cc_OFHTMap(b *testing.B) {
	m := cc.NewOFHTMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_cc_FunnelMap(b *testing.B) {
	m := cc.NewFunnelMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_ccMap(b *testing.B) {
	m := cc.NewMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_ccFlatMap(b *testing.B) {
	m := cc.NewFlatMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_syncMap(b *testing.B) {
	var m sync.Map
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_skipmap(b *testing.B) {
	m := skipmap.New[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_cc_skipmap(b *testing.B) {
	m := cc.NewSkipMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_haxmap(b *testing.B) {
	m := haxmap.New[int, int]()
	for _, key := range preGenIntKeys {
		m.Set(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Get(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_xsyncMap(b *testing.B) {
	m := xsync.NewMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_CsMap(b *testing.B) {
	m := csmap.New[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntLoad_RWShardedMap(b *testing.B) {
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

// ============================================================================
// Int Key - Delete (continuous Store + Delete)
// ============================================================================

func BenchmarkConcurrent_IntDelete_cc_DWHTMap(b *testing.B) {
	m := cc.NewDWHTMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_cc_OFHTMap(b *testing.B) {
	m := cc.NewOFHTMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_cc_FunnelMap(b *testing.B) {
	m := cc.NewFunnelMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_ccMap(b *testing.B) {
	m := cc.NewMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_ccFlatMap(b *testing.B) {
	m := cc.NewFlatMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_syncMap(b *testing.B) {
	var m sync.Map
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_skipmap(b *testing.B) {
	m := skipmap.New[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_cc_skipmap(b *testing.B) {
	m := cc.NewSkipMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_haxmap(b *testing.B) {
	m := haxmap.New[int, int]()
	for _, key := range preGenIntKeys {
		m.Set(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Del(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_xsyncMap(b *testing.B) {
	m := xsync.NewMap[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_CsMap(b *testing.B) {
	m := csmap.New[int, int]()
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_IntDelete_RWShardedMap(b *testing.B) {
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	for _, key := range preGenIntKeys {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenIntKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

// ============================================================================
// Int Key - Range (concurrent read all data)
// ============================================================================

func BenchmarkConcurrent_IntRange_cc_DWHTMap(b *testing.B) {
	m := cc.NewDWHTMap[int, int]()
	for _, key := range preGenIntKeys[:1000] { // Range test uses less data
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_cc_OFHTMap(b *testing.B) {
	m := cc.NewOFHTMap[int, int]()
	for _, key := range preGenIntKeys[:1000] { // Range test uses less data
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_cc_FunnelMap(b *testing.B) {
	m := cc.NewFunnelMap[int, int]()
	for _, key := range preGenIntKeys[:1000] { // Range test uses less data
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_ccMap(b *testing.B) {
	m := cc.NewMap[int, int]()
	for _, key := range preGenIntKeys[:1000] { // Range test uses less data
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_ccFlatMap(b *testing.B) {
	m := cc.NewFlatMap[int, int]()
	for _, key := range preGenIntKeys[:1000] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_syncMap(b *testing.B) {
	var m sync.Map
	for _, key := range preGenIntKeys[:1000] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v any) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_skipmap(b *testing.B) {
	m := skipmap.New[int, int]()
	for _, key := range preGenIntKeys[:1000] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_cc_skipmap(b *testing.B) {
	m := cc.NewSkipMap[int, int]()
	for _, key := range preGenIntKeys[:1000] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_haxmap(b *testing.B) {
	m := haxmap.New[int, int]()
	for _, key := range preGenIntKeys[:1000] {
		m.Set(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.ForEach(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_xsyncMap(b *testing.B) {
	m := xsync.NewMap[int, int]()
	for _, key := range preGenIntKeys[:1000] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.RangeRelaxed(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_CsMap(b *testing.B) {
	m := csmap.New[int, int]()
	for _, key := range preGenIntKeys[:1000] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_IntRange_RWShardedMap(b *testing.B) {
	m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	for _, key := range preGenIntKeys[:1000] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v int) bool { return true })
		}
	})
}

// ============================================================================
// Int Key - MatchDelete
// ============================================================================

func BenchmarkConcurrent_IntMatchDelete_XsyncMap(b *testing.B) {
	m := xsync.NewMap[int, int]()
	for _, key := range preGenIntKeys[:] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.DeleteMatching(func(key int, value int) (delete, stop bool) {
				return true, false
			})
		}
	})
}

func BenchmarkConcurrent_IntMatchDelete_CCMap(b *testing.B) {
	m := cc.NewMap[int, int]()
	for _, key := range preGenIntKeys[:] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for e := range m.Entries() {
				e.Delete()
			}
			m.Shrink()
		}
	})
}

func BenchmarkConcurrent_IntMatchDelete_CCFlatMap(b *testing.B) {
	m := cc.NewFlatMap[int, int]()
	for _, key := range preGenIntKeys[:] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for e := range m.Entries() {
				e.Delete()
			}
			m.Shrink()
		}
	})
}

func BenchmarkConcurrent_IntMatchDelete_CCFunnelMap(b *testing.B) {
	m := cc.NewFunnelMap[int, int]()
	for _, key := range preGenIntKeys[:] {
		m.Store(key, key)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for e := range m.Entries() {
				e.Delete()
			}
			m.Shrink()
		}
	})
}

// ============================================================================
// String Key - Store
// ============================================================================
func BenchmarkConcurrent_StrStore_cc_DWHTMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewDWHTMap[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_cc_OFHTMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewOFHTMap[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_ccFunnelMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFunnelMap[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_ccMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewMap[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_ccFlatMap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewFlatMap[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_syncMap(b *testing.B) {
	b.ReportAllocs()
	var m sync.Map
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := skipmap.NewString[int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_cc_skipmap(b *testing.B) {
	b.ReportAllocs()
	m := cc.NewSkipMap[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_haxmap(b *testing.B) {
	b.ReportAllocs()
	m := haxmap.New[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Set(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_xsyncMap(b *testing.B) {
	b.ReportAllocs()
	m := xsync.NewMap[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_CsMap(b *testing.B) {
	b.ReportAllocs()
	m := csmap.New[string, int]()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrStore_RWShardedMap(b *testing.B) {
	b.ReportAllocs()
	m := NewRWLockShardedMap[string, int](runtime.GOMAXPROCS(0) * 4)
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Store(key, i)
			i = keyIndex(i + 1)
		}
	})
}

// ============================================================================
// String Key - Load (pre-populated data)
// ============================================================================

func BenchmarkConcurrent_StrLoad_cc_DWHTMap(b *testing.B) {
	m := cc.NewDWHTMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_cc_OFHTMap(b *testing.B) {
	m := cc.NewOFHTMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_ccFunnelMap(b *testing.B) {
	m := cc.NewFunnelMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_ccMap(b *testing.B) {
	m := cc.NewMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_ccFlatMap(b *testing.B) {
	m := cc.NewFlatMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_syncMap(b *testing.B) {
	var m sync.Map
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_skipmap(b *testing.B) {
	m := skipmap.NewString[int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_cc_skipmap(b *testing.B) {
	m := cc.NewSkipMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_haxmap(b *testing.B) {
	m := haxmap.New[string, int]()
	for i, key := range preGenStringKeys {
		m.Set(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Get(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_xsyncMap(b *testing.B) {
	m := xsync.NewMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_CsMap(b *testing.B) {
	m := csmap.New[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrLoad_RWShardedMap(b *testing.B) {
	m := NewRWLockShardedMap[string, int](runtime.GOMAXPROCS(0) * 4)
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			_, _ = m.Load(key)
			i = keyIndex(i + 1)
		}
	})
}

// ============================================================================
// String Key - Delete
// ============================================================================

func BenchmarkConcurrent_StrDelete_cc_DWHTMap(b *testing.B) {
	m := cc.NewDWHTMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_cc_OFHTMap(b *testing.B) {
	m := cc.NewOFHTMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_ccFunnelMap(b *testing.B) {
	m := cc.NewFunnelMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_ccMap(b *testing.B) {
	m := cc.NewMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_ccFlatMap(b *testing.B) {
	m := cc.NewFlatMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_syncMap(b *testing.B) {
	var m sync.Map
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_skipmap(b *testing.B) {
	m := skipmap.NewString[int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_cc_skipmap(b *testing.B) {
	m := cc.NewSkipMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_haxmap(b *testing.B) {
	m := haxmap.New[string, int]()
	for i, key := range preGenStringKeys {
		m.Set(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Del(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_xsyncMap(b *testing.B) {
	m := xsync.NewMap[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_CsMap(b *testing.B) {
	m := csmap.New[string, int]()
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

func BenchmarkConcurrent_StrDelete_RWShardedMap(b *testing.B) {
	m := NewRWLockShardedMap[string, int](runtime.GOMAXPROCS(0) * 4)
	for i, key := range preGenStringKeys {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(runtime_cheaprand()) % benchKeyCount
		for pb.Next() {
			key := preGenStringKeys[i]
			m.Delete(key)
			i = keyIndex(i + 1)
		}
	})
}

// ============================================================================
// String Key - Range
// ============================================================================

func BenchmarkConcurrent_StrRange_cc_DWHTMap(b *testing.B) {
	m := cc.NewDWHTMap[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_cc_OFHTMap(b *testing.B) {
	m := cc.NewOFHTMap[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_ccFunnelMap(b *testing.B) {
	m := cc.NewFunnelMap[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_ccMap(b *testing.B) {
	m := cc.NewMap[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_ccFlatMap(b *testing.B) {
	m := cc.NewFlatMap[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_syncMap(b *testing.B) {
	var m sync.Map
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k, v any) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_skipmap(b *testing.B) {
	m := skipmap.NewString[int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_cc_skipmap(b *testing.B) {
	m := cc.NewSkipMap[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_haxmap(b *testing.B) {
	m := haxmap.New[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Set(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.ForEach(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_xsyncMap(b *testing.B) {
	m := xsync.NewMap[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.RangeRelaxed(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_CsMap(b *testing.B) {
	m := csmap.New[string, int]()
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

func BenchmarkConcurrent_StrRange_RWShardedMap(b *testing.B) {
	m := NewRWLockShardedMap[string, int](runtime.GOMAXPROCS(0) * 4)
	for i, key := range preGenStringKeys[:1000] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Range(func(k string, v int) bool { return true })
		}
	})
}

// ============================================================================
// String Key - MatchDelete
// ============================================================================

func BenchmarkConcurrent_StrMatchDelete_XsyncMap(b *testing.B) {
	m := xsync.NewMap[string, int]()
	for i, key := range preGenStringKeys[:] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.DeleteMatching(func(key string, value int) (delete, stop bool) {
				return true, false
			})
		}
	})
}

func BenchmarkConcurrent_StrMatchDelete_CCMap(b *testing.B) {
	m := cc.NewMap[string, int]()
	for i, key := range preGenStringKeys[:] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for e := range m.Entries() {
				e.Delete()
			}
			m.Shrink()
		}
	})
}

func BenchmarkConcurrent_StrMatchDelete_CCFlatMap(b *testing.B) {
	m := cc.NewFlatMap[string, int]()
	for i, key := range preGenStringKeys[:] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for e := range m.Entries() {
				e.Delete()
			}
			m.Shrink()
		}
	})
}

func BenchmarkConcurrent_StrMatchDelete_CCFunnelMap(b *testing.B) {
	m := cc.NewFunnelMap[string, int]()
	for i, key := range preGenStringKeys[:] {
		m.Store(key, i)
	}
	b.ReportAllocs()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for e := range m.Entries() {
				e.Delete()
			}
			m.Shrink()
		}
	})
}
