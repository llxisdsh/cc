package benchmark

import (
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/llxisdsh/cc"
)

// badHash forces 100% hash collisions, causing all keys to degrade into a single bucket.
func badHash(ptr unsafe.Pointer, seed uintptr) uintptr {
	return 0xbadcafe
}

func testBadHashInsert_cc_FunnelMap(t *testing.T, total int, numCPU int) {
	time.Sleep(1 * time.Second)
	runtime.GC()

	m := cc.NewFunnelMap[int, int](
		cc.WithCapacity(total),
		cc.WithKeyHasherUnsafe(badHash),
	)

	var wg sync.WaitGroup
	wg.Add(numCPU)
	start := time.Now()
	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed := time.Since(start)

	if m.Size() != total {
		t.Errorf("Expected size %d, got %d", total, m.Size())
	}
	t.Logf("----------------------------------")
	t.Logf("FunnelMap Inserted %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)

	// test Load
	time.Sleep(1 * time.Second)
	runtime.GC()
	wg.Add(numCPU)
	start = time.Now()
	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Load(j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed = time.Since(start)
	t.Logf("----------------------------------")
	t.Logf("FunnelMap Loaded %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)
}

func testBadHashInsert_cc_Map(t *testing.T, total int, numCPU int) {
	time.Sleep(1 * time.Second)
	runtime.GC()

	m := cc.NewMap[int, int](
		cc.WithCapacity(total),
		cc.WithKeyHasherUnsafe(badHash),
	)

	var wg sync.WaitGroup
	wg.Add(numCPU)
	start := time.Now()
	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed := time.Since(start)

	if m.Size() != total {
		t.Errorf("Expected size %d, got %d", total, m.Size())
	}
	t.Logf("----------------------------------")
	t.Logf("Map Inserted %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)

	// test Load
	time.Sleep(1 * time.Second)
	runtime.GC()
	wg.Add(numCPU)
	start = time.Now()
	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Load(j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed = time.Since(start)
	t.Logf("----------------------------------")
	t.Logf("Map Loaded %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)
}

func testBadHashInsert_cc_DWHTMap(t *testing.T, total int, numCPU int) {
	time.Sleep(1 * time.Second)
	runtime.GC()

	m := cc.NewDWHTMap[int, int](
		cc.WithCapacity(total),
		cc.WithKeyHasherUnsafe(badHash),
	)

	var wg sync.WaitGroup
	wg.Add(numCPU)
	start := time.Now()
	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed := time.Since(start)

	if m.Size() != total {
		t.Errorf("Expected size %d, got %d", total, m.Size())
	}
	t.Logf("----------------------------------")
	t.Logf("DWHTMap Inserted %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)

	// test Load
	time.Sleep(1 * time.Second)
	runtime.GC()
	wg.Add(numCPU)
	start = time.Now()
	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Load(j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed = time.Since(start)
	t.Logf("----------------------------------")
	t.Logf("DWHTMap Loaded %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)
}

func testBadHashInsert_cc_OFHTMap(t *testing.T, total int, numCPU int) {
	time.Sleep(1 * time.Second)
	runtime.GC()

	m := cc.NewOFHTMap[int, int](
		cc.WithCapacity(total),
		cc.WithKeyHasherUnsafe(badHash),
	)

	var wg sync.WaitGroup
	wg.Add(numCPU)
	start := time.Now()
	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed := time.Since(start)

	if m.Size() != total {
		t.Errorf("Expected size %d, got %d", total, m.Size())
	}
	t.Logf("----------------------------------")
	t.Logf("OFHTMap Inserted %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)

	// test Load
	time.Sleep(1 * time.Second)
	runtime.GC()
	wg.Add(numCPU)
	start = time.Now()
	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Load(j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed = time.Since(start)
	t.Logf("----------------------------------")
	t.Logf("OFHTMap Loaded %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)
}

func testBadHashInsert_cc_FlatMap(t *testing.T, total int, numCPU int) {
	time.Sleep(1 * time.Second)
	runtime.GC()

	m := cc.NewFlatMap[int, int](
		cc.WithCapacity(total),
		cc.WithKeyHasherUnsafe(badHash),
	)

	var wg sync.WaitGroup
	wg.Add(numCPU)
	start := time.Now()
	batchSize := (total + numCPU - 1) / numCPU

	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Store(j, j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed := time.Since(start)

	if m.Size() != total {
		t.Errorf("Expected size %d, got %d", total, m.Size())
	}
	t.Logf("----------------------------------")
	t.Logf("FlatMap Inserted %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)

	// test Load
	time.Sleep(1 * time.Second)
	runtime.GC()
	wg.Add(numCPU)
	start = time.Now()
	for i := range numCPU {
		go func(start, end int) {
			for j := start; j < end; j++ {
				m.Load(j)
			}
			wg.Done()
		}(i*batchSize, min((i+1)*batchSize, total))
	}
	wg.Wait()
	elapsed = time.Since(start)
	t.Logf("----------------------------------")
	t.Logf("FlatMap Loaded %d items with BAD HASH in %v", total, elapsed)
	t.Logf("Average: %.2f ns/op", float64(elapsed.Nanoseconds())/float64(total))
	t.Logf(
		"Throughput: %.2f ops/sec",
		float64(total)/elapsed.Seconds(),
	)
}

func TestBadHash_Performance(t *testing.T) {
	// Since map.go creates a linked list of length N during complete hash collisions,
	// resulting in O(N^2) time complexity.
	// It is recommended to set this to around 20,000 to clearly observe the massive
	// throughput gap. Setting it too high will cause map.go to hang indefinitely.
	total := 100000
	t.Run("FunnelMap_BadHash", func(t *testing.T) {
		testBadHashInsert_cc_FunnelMap(t, total, runtime.GOMAXPROCS(0))
	})
	t.Run("Map_BadHash", func(t *testing.T) {
		testBadHashInsert_cc_Map(t, total, runtime.GOMAXPROCS(0))
	})
	t.Run("DWHTMap_BadHash", func(t *testing.T) {
		testBadHashInsert_cc_DWHTMap(t, total, runtime.GOMAXPROCS(0))
	})
	t.Run("OFHTMap_BadHash", func(t *testing.T) {
		testBadHashInsert_cc_OFHTMap(t, total, runtime.GOMAXPROCS(0))
	})
	t.Run("FlatMap_BadHash", func(t *testing.T) {
		testBadHashInsert_cc_FlatMap(t, total, runtime.GOMAXPROCS(0))
	})
}
