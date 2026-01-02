package benchmark

import (
	"runtime"
	"sync"
	"testing"

	"github.com/alphadose/haxmap"

	"github.com/zhangyunhao116/skipmap"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/llxisdsh/cc"
)

func Test_MemoryPeakReduction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	const numItems = 1000

	{
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		m := cc.NewMap[int, int]()

		for i := range numItems {
			m.Store(i, i)
		}

		runtime.ReadMemStats(&m2)

		peak := max(int64(m2.Alloc)-int64(m1.Alloc), 0)

		t.Logf("cc_Map memory usage: %d bytes, items: %d", peak, m.Size())
	}

	{
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		m := cc.NewFlatMap[int, int]()

		for i := range numItems {
			m.Store(i, i)
		}

		runtime.ReadMemStats(&m2)

		peak := max(int64(m2.Alloc)-int64(m1.Alloc), 0)

		t.Logf("cc_FlatMap memory usage: %d bytes, items: %d", peak, m.Size())
	}

	{
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		m := make(map[int]int)

		for i := range numItems {
			m[i] = i
		}

		runtime.ReadMemStats(&m2)
		peak := max(int64(m2.Alloc)-int64(m1.Alloc), 0)

		t.Logf("map memory usage: %d bytes, items: %d", peak, len(m))
	}

	{
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		m := NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)

		for i := range numItems {
			m.Store(i, i)
		}

		runtime.ReadMemStats(&m2)
		peak := max(int64(m2.Alloc)-int64(m1.Alloc), 0)

		t.Logf(
			"RWLockShardedMap memory usage: %d bytes, items: %d",
			peak,
			m.Size(),
		)
	}

	{
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		m := &sync.Map{}

		for i := range numItems {
			m.Store(i, i)
		}

		runtime.ReadMemStats(&m2)
		peak := max(int64(m2.Alloc)-int64(m1.Alloc), 0)

		v, _ := m.Load(numItems - 1)
		t.Logf("sync_Map memory usage: %d bytes, items: %d", peak, v.(int)+1)
	}

	{
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		m := xsync.NewMap[int, int]()

		for i := range numItems {
			m.Store(i, i)
		}

		runtime.ReadMemStats(&m2)
		peak := max(int64(m2.Alloc)-int64(m1.Alloc), 0)

		t.Logf("xsync_MapV4 memory usage: %d bytes, items: %d", peak, m.Size())
	}

	{
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		m := skipmap.New[int, int]()

		for i := range numItems {
			m.Store(i, i)
		}

		runtime.ReadMemStats(&m2)
		peak := max(int64(m2.Alloc)-int64(m1.Alloc), 0)

		t.Logf(
			"zhangyunhao116_skipmap memory usage: %d bytes, items: %d",
			peak,
			m.Len(),
		)
	}

	{
		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		m := haxmap.New[int, int]()

		for i := range numItems {
			m.Set(i, i)
		}

		runtime.ReadMemStats(&m2)
		peak := max(int64(m2.Alloc)-int64(m1.Alloc), 0)

		t.Logf(
			"alphadose_haxmap memory usage: %d bytes, items: %d",
			peak,
			m.Len(),
		)
	}
}
