package benchmark

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/llxisdsh/cc"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/zhangyunhao116/skipmap"
)

const total = 100_000_000

func TestInsert_cc_FlatMap(t *testing.T) {
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_cc_FlatMap(t, total, 1, false, true, true)
	})
	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsert_cc_FlatMap(t, total, runtime.GOMAXPROCS(0), false, true, false)
	})
	t.Run("1 pre_size", func(t *testing.T) {
		testInsert_cc_FlatMap(t, total, 1, true, false, false)
	})
	t.Run("64 pre_size", func(t *testing.T) {
		testInsert_cc_FlatMap(t, total, runtime.GOMAXPROCS(0), true, false, false)
	})
}

func testInsert_cc_FlatMap(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
	testLoad bool,
	testDelete bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()

	var m *cc.FlatMap[int, int]
	if preSize {
		m = cc.NewFlatMap[int, int](cc.WithCapacity(total))
	} else {
		m = cc.NewFlatMap[int, int]()
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m.Store(j, j)

				// m.Compute(
				// 	j,
				// 	func(e *cc.MapEntry[int, int]) {
				// 		e.Update(j)
				// 	},
				// )
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)
	t.Logf("----------------------------------")
	size := m.Size()
	if size != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}
	// t.Logf("cap  %v", m.Stats())
	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Load(idx); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}

	if testLoad {
		time.Sleep(2 * time.Second)
		var wg sync.WaitGroup
		wg.Add(numCPU)
		start := time.Now()

		batchSize := (total + numCPU - 1) / numCPU

		for i := range numCPU {
			go func(start, end int) {
				// defer wg.Done()

				for j := start; j < end; j++ {
					_, _ = m.Load(j)
				}
				wg.Done()
			}(i*batchSize, min((i+1)*batchSize, total))
		}
		wg.Wait()
		elapsed := time.Since(start)
		t.Logf("----------------------------------")
		t.Logf("Load %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
	}

	if testDelete {
		var wg sync.WaitGroup
		wg.Add(numCPU)

		start := time.Now()
		for e := range m.Entries() {
			e.Delete()
		}
		elapsed := time.Since(start)
		t.Logf("----------------------------------")
		t.Logf("TestDelete %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
		if m.Size() != 0 {
			t.Errorf("Map is not zero after TestDelete, size: %d", m.Size())
		}
	}
}

func TestInsert_cc_Map(t *testing.T) {
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_cc_Map(t, total, 1, false, true, true)
	})
	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsert_cc_Map(t, total, runtime.GOMAXPROCS(0), false, true, false)
	})
	t.Run("1 pre_size", func(t *testing.T) {
		testInsert_cc_Map(t, total, 1, true, false, false)
	})
	t.Run("64 pre_size", func(t *testing.T) {
		testInsert_cc_Map(t, total, runtime.GOMAXPROCS(0), true, false, false)
	})
}

type Int int

// func (s Int) HashCode(uintptr) uintptr {
//	//n := uint64(s)
//	//return (uintptr)(bits.ReverseBytes64(n))
//	return uintptr(s)
// }
//
// func (s Int) HashOpts() []pb.HashOptimization {
//	return []pb.HashOptimization{pb.LinearDistribution}
// }

func testInsert_cc_Map(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
	testLoad bool,
	testDelete bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()

	var m *cc.Map[Int, int]
	if preSize {
		m = cc.NewMap[Int, int](cc.WithCapacity(total))
	} else {
		m = cc.NewMap[Int, int]()
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()
			// t.Logf("start, %04d, %04d", start, end)
			for j := start; j < end; j++ {
				m.Store(Int(j), j)
				// m.Compute(
				// 	Int(j),
				// 	func(e *cc.MapEntry[Int, int]) {
				// 		e.Update(j)
				// 	},
				// )
				// t.Logf("%04d", j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)

	size := m.Size()
	if size != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}
	t.Logf("----------------------------------")
	// t.Logf("cap  %v", m.Stats())
	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// t.Log(m.Stats())

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Load(Int(idx)); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}

	if testLoad {
		time.Sleep(2 * time.Second)
		var wg sync.WaitGroup
		wg.Add(numCPU)
		start := time.Now()

		batchSize := (total + numCPU - 1) / numCPU
		for i := range numCPU {
			go func(start, end int) {
				// defer wg.Done()

				for j := start; j < end; j++ {
					_, _ = m.Load(Int(j))
				}
				wg.Done()
			}(i*batchSize, min((i+1)*batchSize, total))
		}
		wg.Wait()
		elapsed := time.Since(start)
		t.Logf("----------------------------------")
		t.Logf("Load %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
	}

	if testDelete {
		var wg sync.WaitGroup
		wg.Add(numCPU)

		start := time.Now()
		for e := range m.Entries() {
			e.Delete()
		}
		elapsed := time.Since(start)
		t.Logf("----------------------------------")
		t.Logf("TestDelete %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
		if m.Size() != 0 {
			t.Errorf("Map is not zero after TestDelete, size: %d", m.Size())
		}
	}
}

func TestInsertString_cc_FlatMap(t *testing.T) {
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsertString_cc_FlatMap(t, total, 1, false, true)
	})
	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsertString_cc_FlatMap(
			t,
			total,
			runtime.GOMAXPROCS(0),
			false,
			true,
		)
	})
	t.Run("1 pre_size", func(t *testing.T) {
		testInsertString_cc_FlatMap(t, total, 1, true, false)
	})
	t.Run("64 pre_size", func(t *testing.T) {
		testInsertString_cc_FlatMap(
			t,
			total,
			runtime.GOMAXPROCS(0),
			true,
			false,
		)
	})
}

func testInsertString_cc_FlatMap(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
	testLoad bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()

	var m *cc.FlatMap[string, int]
	if preSize {
		m = cc.NewFlatMap[string, int](cc.WithCapacity(total))
	} else {
		m = cc.NewFlatMap[string, int]()
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m.Store(strconv.Itoa(j), j)

				// m.Compute(
				// 	strconv.Itoa(j),
				// 	func(e *cc.MapEntry[string, int]) {
				// 		e.Update(j)
				// 	},
				// )
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)

	size := m.Size()
	if size != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}
	// t.Logf("cap  %v", m.Stats())
	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Load(strconv.Itoa(idx)); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}

	if testLoad {
		var wg sync.WaitGroup
		wg.Add(numCPU)
		start := time.Now()

		batchSize := (total + numCPU - 1) / numCPU

		for i := range numCPU {
			go func(start, end int) {
				// defer wg.Done()

				for j := start; j < end; j++ {
					_, _ = m.Load(strconv.Itoa(j))
				}
				wg.Done()
			}(i*batchSize, min((i+1)*batchSize, total))
		}
		wg.Wait()
		elapsed := time.Since(start)
		t.Logf("----------------------------------")
		t.Logf("Load %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
	}
}

func TestInsertString_cc_Map(t *testing.T) {
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsertString_cc_Map(t, total, 1, false, true)
	})
	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsertString_cc_Map(t, total, runtime.GOMAXPROCS(0), false, true)
	})
	t.Run("1 pre_size", func(t *testing.T) {
		testInsertString_cc_Map(t, total, 1, true, false)
	})
	t.Run("64 pre_size", func(t *testing.T) {
		testInsertString_cc_Map(t, total, runtime.GOMAXPROCS(0), true, false)
	})
}

type String string

// func (*String) HashCodeUnsafe(value unsafe.Pointer, seed uintptr) uintptr {
//	key := *(*[]byte)(value)
//	if len(key) <= 12 {
//		for _, c := range key {
//			seed = seed*31 + uintptr(c)
//		}
//		return seed
//	}
//	return pb.GetBuiltInHasher[string]()(value, seed)
// }

// func (s *String) HashCode(seed uintptr) uintptr {
//	for _, c := range *s {
//		seed = seed*31 + uintptr(c)
//	}
//	return seed
//	// return buildInStringHasher(value, seed)
// }

func testInsertString_cc_Map(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
	testLoad bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()

	var m *cc.Map[String, int]
	if preSize {
		m = cc.NewMap[String, int](cc.WithCapacity(total))
	} else {
		m = cc.NewMap[String, int]()
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m.Store(String(strconv.Itoa(j)), j)
				// m.Compute(
				//	j,
				//	func(*MapEntry[int, int]) (*MapEntry[int, int], int, bool) {
				//		return &MapEntry[int, int]{Value: j}, 0, false
				//	},
				// )
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)

	size := m.Size()
	if size != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}
	// t.Logf("cap  %v", m.Stats())
	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Load(String(strconv.Itoa(idx))); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}

	if testLoad {
		var wg sync.WaitGroup
		wg.Add(numCPU)
		start := time.Now()

		batchSize := (total + numCPU - 1) / numCPU

		for i := range numCPU {
			go func(start, end int) {
				// defer wg.Done()

				for j := start; j < end; j++ {
					_, _ = m.Load(String(strconv.Itoa(j)))
				}
				wg.Done()
			}(i*batchSize, min((i+1)*batchSize, total))
		}
		wg.Wait()
		elapsed := time.Since(start)
		t.Logf("Load %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
	}
}

func TestInsert_xsync_MapV4(t *testing.T) {
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_xsync_MapV4(t, total, 1, false, true, true)
	})

	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsert_xsync_MapV4(t, total, runtime.GOMAXPROCS(0), false, true, false)
	})
	t.Run("1 pre_size", func(t *testing.T) {
		testInsert_xsync_MapV4(t, total, 1, true, false, false)
	})

	t.Run("64 pre_size", func(t *testing.T) {
		testInsert_xsync_MapV4(t, total, runtime.GOMAXPROCS(0), true, false, false)
	})
}

func testInsert_xsync_MapV4(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
	testLoad bool,
	testDelete bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()

	var m *xsync.Map[int, int]
	if preSize {
		m = xsync.NewMap[int, int](xsync.WithPresize(total))
	} else {
		m = xsync.NewMap[int, int]()
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)

	size := m.Size()
	if size != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}
	t.Logf("cap  %v", m.Stats().RootBuckets)
	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Load(idx); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}

	if testLoad {
		time.Sleep(2 * time.Second)
		var wg sync.WaitGroup
		wg.Add(numCPU)
		start := time.Now()

		batchSize := (total + numCPU - 1) / numCPU

		for i := range numCPU {
			go func(start, end int) {
				// defer wg.Done()

				for j := start; j < end; j++ {
					_, _ = m.Load(j)
				}
				wg.Done()
			}(i*batchSize, min((i+1)*batchSize, total))
		}
		wg.Wait()
		elapsed := time.Since(start)
		t.Logf("Load %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
	}

	if testDelete {
		var wg sync.WaitGroup
		wg.Add(numCPU)

		start := time.Now()
		for k := range m.Range {
			m.Delete(k)
		}
		elapsed := time.Since(start)
		t.Logf("----------------------------------")
		t.Logf("TestDelete %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
		if m.Size() != 0 {
			t.Errorf("Map is not zero after TestDelete, size: %d", m.Size())
		}
	}
}

func TestInsert_RWLockShardedMap(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping heavy RWLockShardedMap insert test in short mode")
	}
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_RWLockShardedMap(t, total, 1, false, true)
	})

	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsert_RWLockShardedMap(
			t,
			total,
			runtime.GOMAXPROCS(0),
			false,
			true,
		)
	})
}

func testInsert_RWLockShardedMap(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
	testLoad bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()
	var m *RWLockShardedMap[string, int]
	if preSize {
		m = NewRWLockShardedMap[string, int](runtime.GOMAXPROCS(0) * 4)
	} else {
		m = NewRWLockShardedMap[string, int](runtime.GOMAXPROCS(0) * 4)
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m.Store(strconv.Itoa(j), j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)
	size := m.Size()
	// m.Range(func(i int, i2 int) bool {
	//	size++
	//	return true
	// })

	if size != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}
	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Load(strconv.Itoa(idx)); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}

	if testLoad {
		var wg sync.WaitGroup
		wg.Add(numCPU)
		start := time.Now()

		batchSize := (total + numCPU - 1) / numCPU

		for i := range numCPU {
			go func(start, end int) {
				// defer wg.Done()

				for j := start; j < end; j++ {
					_, _ = m.Load(strconv.Itoa(j))
				}
				wg.Done()
			}(i*batchSize, min((i+1)*batchSize, total))
		}
		wg.Wait()
		elapsed := time.Since(start)
		t.Logf("Load %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
	}
}

func TestInsert_original_syncMap(t *testing.T) {
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_original_syncMap(t, total, 1, false)
	})

	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsert_original_syncMap(t, total, runtime.GOMAXPROCS(0), false)
	})
}

func testInsert_original_syncMap(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()
	var m *sync.Map
	if preSize {
		m = &sync.Map{}
	} else {
		m = &sync.Map{}
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)

	// size := m.Size()
	// if size != total {
	//	t.Errorf("Expected size %d, got %d", total, size)
	// }

	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Load(idx); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}
}

func TestInsert_alphadose_haxmap(t *testing.T) {
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_alphadose_haxmap(t, total, 1, false)
	})

	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsert_alphadose_haxmap(t, total, runtime.GOMAXPROCS(0), false)
	})
	t.Run("1 pre_size", func(t *testing.T) {
		testInsert_alphadose_haxmap(t, total, 1, true)
	})

	t.Run("64 pre_size", func(t *testing.T) {
		testInsert_alphadose_haxmap(t, total, runtime.GOMAXPROCS(0), true)
	})
}

func testInsert_alphadose_haxmap(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()
	var m *haxmap.Map[int, int]
	if preSize {
		m = haxmap.New[int, int](uintptr(total))
	} else {
		m = haxmap.New[int, int]()
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m.Set(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)

	size := m.Len()
	if int(size) != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}

	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Get(idx); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}
}

func TestInsert_zhangyunhao116_skipmap(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping heavy skipmap insert test in short mode")
	}
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_zhangyunhao116_skipmap(t, total, 1, false)
	})

	t.Run("64 no_pre_size", func(t *testing.T) {
		testInsert_zhangyunhao116_skipmap(
			t,
			total,
			runtime.GOMAXPROCS(0),
			false,
		)
	})
	// t.Run("1 pre_size", func(t *testing.T) {
	//	testInsert_zhangyunhao116_skipmap(t, total, 1, true)
	// })
	//
	// t.Run("64 pre_size", func(t *testing.T) {
	// 	testInsert_zhangyunhao116_skipmap(t, total, runtime.GOMAXPROCS(0), true)
	// })
}

func testInsert_zhangyunhao116_skipmap(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()
	var m *skipmap.OrderedMap[int, int]
	if preSize {
		m = skipmap.New[int, int]()
	} else {
		m = skipmap.New[int, int]()
	}

	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)

	size := m.Len()
	if int(size) != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}

	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m.Load(idx); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}
}

func TestInsert_hashMaps(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping heavy insert test in short mode")
	}
	t.Run("1 no_pre_size", func(t *testing.T) {
		testInsert_hashMaps(t, total, 1, false, true)
	})

	// t.Run("64 no_pre_size", func(t *testing.T) {
	//	testInsert_RWLockShardedMap(
	//		t,
	//		total,
	//		runtime.GOMAXPROCS(0),
	//		false,
	//		true,
	//	)
	// })
}

func testInsert_hashMaps(
	t *testing.T,
	total int,
	numCPU int,
	preSize bool,
	testLoad bool,
) {
	time.Sleep(2 * time.Second)
	runtime.GC()
	// var m *hashmaps.HashMap[int, int]
	// if preSize {
	//	m = robin.New[int,int]()
	// } else {
	//	m = NewRWLockShardedMap[int, int](runtime.GOMAXPROCS(0) * 4)
	// }

	// m := robin.New[string, int]()
	// m := hopscotch.New[int, int]()
	// m := unordered.New[int, int]()
	m := make(map[string]int)
	// m := swiss.New[string, int](100)
	// m := pb.NewMap[string, int]()
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		func(start, end int) {
			// defer wg.Done()

			for j := start; j < end; j++ {
				m[strconv.Itoa(j)] = j
				// m.Put(strconv.Itoa(j), j)
				// m.Store(strconv.Itoa(j), j)
				// m.Compute(
				//	strconv.Itoa(j),
				// 	func(*MapEntry[string, int]) (*MapEntry[string, int], int,
				// bool) {
				//		return &MapEntry[string, int]{Value: j}, 0, false
				//	},
				// )
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}

	wg.Wait()

	elapsed := time.Since(start)
	size := 0
	// m.Each(func(i int, i2 int) bool {
	//	size++
	//	return true
	// })
	size = len(m)
	// size = m.Len()
	// size = m.Size()
	// size = m.Count()

	if size != total {
		t.Errorf("Expected size %d, got %d", total, size)
	}
	t.Logf("Inserted %d items in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f million ops/sec",
		float64(total)/(elapsed.Seconds()*1000000),
	)

	// rand check
	for i := range 1000 {
		idx := i * (total / 1000)
		if val, ok := m[strconv.Itoa(idx)]; !ok || val != idx {
			// if val, ok := m.Get(strconv.Itoa(idx)); !ok || val != idx {
			// if val, ok := m.Load(strconv.Itoa(idx)); !ok || val != idx {
			// if val, ok := m.(strconv.Itoa(idx)); !ok || val != idx {
			t.Errorf(
				"Expected value %d at key %d, got %d, exists: %v",
				idx,
				idx,
				val,
				ok,
			)
		}
	}

	if testLoad {
		var wg sync.WaitGroup
		wg.Add(numCPU)
		start := time.Now()

		batchSize := (total + numCPU - 1) / numCPU

		for i := range numCPU {
			go func(start, end int) {
				// defer wg.Done()

				for j := start; j < end; j++ {
					_, _ = m[strconv.Itoa(j)]
					// _, _ = m.Get(strconv.Itoa(j))
					// _, _ = m.Load(strconv.Itoa(j))
				}
				wg.Done()
			}(i*batchSize, min((i+1)*batchSize, total))
		}
		wg.Wait()
		elapsed := time.Since(start)
		t.Logf("Load %d items in %v", total, elapsed)
		t.Logf(
			"Average: %.2f ns/op",
			float64(elapsed.Nanoseconds())/float64(total),
		)
		t.Logf(
			"Throughput: %.2f million ops/sec",
			float64(total)/(elapsed.Seconds()*1000000),
		)
	}
}
