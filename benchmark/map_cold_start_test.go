package benchmark

import (
	"runtime"
	"sync"
	"testing"
	"time"
	_ "unsafe"

	"github.com/alphadose/haxmap"
	"github.com/llxisdsh/cc"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/zhangyunhao116/skipmap"
)

//go:noescape
//go:linkname runtime_cheaprand runtime.cheaprand
func runtime_cheaprand() uint32

// getMemUsage gets current memory usage (MB)
func getMemUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

const (
	numMaps = 1024 * 1024 // map count, must be power of 2
	testOps = 10_000_000  // test operation count
	numCPU  = 8
)

func TestColdStart_cc_FlatMap(t *testing.T) {
	t.Logf("Initializing %d instances...", numMaps)

	// create array of 1 million map instance pointers
	maps := make([]*cc.FlatMap[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = cc.NewFlatMap[int, int]()
	}

	// force GC to ensure cold cache
	runtime.GC()
	runtime.GC()

	t.Logf("Starting formal test of %d Compute operations...", testOps)
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	for range numCPU {
		go func() {
			for range testOps {
				mapIdx := runtime_cheaprand() & (numMaps - 1)
				key := int(runtime_cheaprand())

				maps[mapIdx].Store(key, key+1)

			}

			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgNs := elapsed.Nanoseconds() / testOps
	opsPerSec := float64(testOps) / elapsed.Seconds()

	t.Logf("Compute operation results:")
	t.Logf("  Total operations: %d", testOps)
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Average latency: %d ns/op", avgNs)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
	t.Logf("  Memory usage: %d MB", getMemUsage())

	t.Logf("Starting formal test of %d Load operations...", testOps)

	// Load test
	{
		time.Sleep(2 * time.Second)
		// force GC to ensure cold cache
		runtime.GC()
		runtime.GC()

		var wg sync.WaitGroup
		wg.Add(numCPU)

		start = time.Now()
		for range numCPU {
			go func() {
				for i := 0; i < testOps; i++ {
					mapIdx := runtime_cheaprand() & (numMaps - 1)
					key := int(runtime_cheaprand())
					_, _ = maps[mapIdx].Load(key)
				}

				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		avgNs := elapsed.Nanoseconds() / testOps
		opsPerSec := float64(testOps) / elapsed.Seconds()

		t.Logf("Load operation results:")
		t.Logf("  Total operations: %d", testOps)
		t.Logf("  Total time: %v", elapsed)
		t.Logf("  Average latency: %d ns/op", avgNs)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		t.Logf("  Memory usage: %d MB", getMemUsage())
	}
}

func TestColdStart_cc_Map(t *testing.T) {
	t.Logf("Initializing %d instances...", numMaps)

	// create array of 1 million map instance pointers
	maps := make([]*cc.Map[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = cc.NewMap[int, int]()
	}

	// force GC to ensure cold cache
	runtime.GC()
	runtime.GC()

	t.Logf("Starting formal test of %d Compute operations...", testOps)
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	for range numCPU {
		go func() {
			for range testOps {
				mapIdx := runtime_cheaprand() & (numMaps - 1)
				key := int(runtime_cheaprand())

				maps[mapIdx].Store(key, key+1)

			}

			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgNs := elapsed.Nanoseconds() / testOps
	opsPerSec := float64(testOps) / elapsed.Seconds()

	t.Logf("Compute operation results:")
	t.Logf("  Total operations: %d", testOps)
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Average latency: %d ns/op", avgNs)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
	t.Logf("  Memory usage: %d MB", getMemUsage())

	t.Logf("Starting formal test of %d Load operations...", testOps)

	// Load test
	{
		time.Sleep(2 * time.Second)
		// force GC to ensure cold cache
		runtime.GC()
		runtime.GC()

		var wg sync.WaitGroup
		wg.Add(numCPU)

		start = time.Now()
		for range numCPU {
			go func() {
				for i := 0; i < testOps; i++ {
					mapIdx := runtime_cheaprand() & (numMaps - 1)
					key := int(runtime_cheaprand())
					_, _ = maps[mapIdx].Load(key)
				}

				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		avgNs := elapsed.Nanoseconds() / testOps
		opsPerSec := float64(testOps) / elapsed.Seconds()

		t.Logf("Load operation results:")
		t.Logf("  Total operations: %d", testOps)
		t.Logf("  Total time: %v", elapsed)
		t.Logf("  Average latency: %d ns/op", avgNs)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		t.Logf("  Memory usage: %d MB", getMemUsage())
	}
}

func TestColdStart_cc_FunnelMap(t *testing.T) {
	t.Logf("Initializing %d instances...", numMaps)

	// create array of 1 million map instance pointers
	maps := make([]*cc.FunnelMap[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = cc.NewFunnelMap[int, int]()
	}

	// force GC to ensure cold cache
	runtime.GC()
	runtime.GC()

	t.Logf("Starting formal test of %d Compute operations...", testOps)
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	for range numCPU {
		go func() {
			for range testOps {
				mapIdx := runtime_cheaprand() & (numMaps - 1)
				key := int(runtime_cheaprand())

				maps[mapIdx].Store(key, key+1)

			}

			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgNs := elapsed.Nanoseconds() / testOps
	opsPerSec := float64(testOps) / elapsed.Seconds()

	t.Logf("Compute operation results:")
	t.Logf("  Total operations: %d", testOps)
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Average latency: %d ns/op", avgNs)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
	t.Logf("  Memory usage: %d MB", getMemUsage())

	t.Logf("Starting formal test of %d Load operations...", testOps)

	// Load test
	{
		time.Sleep(2 * time.Second)
		// force GC to ensure cold cache
		runtime.GC()
		runtime.GC()

		var wg sync.WaitGroup
		wg.Add(numCPU)

		start = time.Now()
		for range numCPU {
			go func() {
				for i := 0; i < testOps; i++ {
					mapIdx := runtime_cheaprand() & (numMaps - 1)
					key := int(runtime_cheaprand())
					_, _ = maps[mapIdx].Load(key)
				}

				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		avgNs := elapsed.Nanoseconds() / testOps
		opsPerSec := float64(testOps) / elapsed.Seconds()

		t.Logf("Load operation results:")
		t.Logf("  Total operations: %d", testOps)
		t.Logf("  Total time: %v", elapsed)
		t.Logf("  Average latency: %d ns/op", avgNs)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		t.Logf("  Memory usage: %d MB", getMemUsage())
	}
}

func TestColdStart_RWLockShardedMap(t *testing.T) {
	t.Logf("Initializing %d instances...", numMaps)

	// create array of 1 million map instance pointers
	maps := make([]*RWLockShardedMap[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = NewRWLockShardedMap[int, int](256)
	}

	// force GC to ensure cold cache
	runtime.GC()
	runtime.GC()

	t.Logf("Starting formal test of %d Compute operations...", testOps)
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	for range numCPU {
		go func() {
			for range testOps {
				mapIdx := runtime_cheaprand() & (numMaps - 1)
				key := int(runtime_cheaprand())

				maps[mapIdx].Store(key, key+1)

			}

			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgNs := elapsed.Nanoseconds() / testOps
	opsPerSec := float64(testOps) / elapsed.Seconds()

	t.Logf("Compute operation results:")
	t.Logf("  Total operations: %d", testOps)
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Average latency: %d ns/op", avgNs)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
	t.Logf("  Memory usage: %d MB", getMemUsage())

	t.Logf("Starting formal test of %d Load operations...", testOps)

	// Load test
	{
		time.Sleep(2 * time.Second)
		// force GC to ensure cold cache
		runtime.GC()
		runtime.GC()

		var wg sync.WaitGroup
		wg.Add(numCPU)

		start = time.Now()
		for range numCPU {
			go func() {
				for i := 0; i < testOps; i++ {
					mapIdx := runtime_cheaprand() & (numMaps - 1)
					key := int(runtime_cheaprand())
					_, _ = maps[mapIdx].Load(key)
				}

				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		avgNs := elapsed.Nanoseconds() / testOps
		opsPerSec := float64(testOps) / elapsed.Seconds()

		t.Logf("Load operation results:")
		t.Logf("  Total operations: %d", testOps)
		t.Logf("  Total time: %v", elapsed)
		t.Logf("  Average latency: %d ns/op", avgNs)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		t.Logf("  Memory usage: %d MB", getMemUsage())
	}
}

func TestColdStart_xsync_Map(t *testing.T) {
	t.Logf("Initializing %d instances...", numMaps)

	// create array of 1 million map instance pointers
	maps := make([]*xsync.Map[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = xsync.NewMap[int, int]()
	}

	// force GC to ensure cold cache
	runtime.GC()
	runtime.GC()

	t.Logf("Starting formal test of %d Compute operations...", testOps)
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	for range numCPU {
		go func() {
			for range testOps {
				mapIdx := runtime_cheaprand() & (numMaps - 1)
				key := int(runtime_cheaprand())

				maps[mapIdx].Store(key, key+1)

			}

			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgNs := elapsed.Nanoseconds() / testOps
	opsPerSec := float64(testOps) / elapsed.Seconds()

	t.Logf("Compute operation results:")
	t.Logf("  Total operations: %d", testOps)
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Average latency: %d ns/op", avgNs)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
	t.Logf("  Memory usage: %d MB", getMemUsage())

	t.Logf("Starting formal test of %d Load operations...", testOps)

	// Load test
	{
		time.Sleep(2 * time.Second)
		// force GC to ensure cold cache
		runtime.GC()
		runtime.GC()

		var wg sync.WaitGroup
		wg.Add(numCPU)

		start = time.Now()
		for range numCPU {
			go func() {
				for i := 0; i < testOps; i++ {
					mapIdx := runtime_cheaprand() & (numMaps - 1)
					key := int(runtime_cheaprand())
					_, _ = maps[mapIdx].Load(key)
				}

				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		avgNs := elapsed.Nanoseconds() / testOps
		opsPerSec := float64(testOps) / elapsed.Seconds()

		t.Logf("Load operation results:")
		t.Logf("  Total operations: %d", testOps)
		t.Logf("  Total time: %v", elapsed)
		t.Logf("  Average latency: %d ns/op", avgNs)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		t.Logf("  Memory usage: %d MB", getMemUsage())
	}
}

func TestColdStart_sync_Map(t *testing.T) {
	t.Logf("Initializing %d instances...", numMaps)

	// create array of 1 million map instance pointers
	maps := make([]*sync.Map, numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = &sync.Map{}
	}

	// force GC to ensure cold cache
	runtime.GC()
	runtime.GC()

	t.Logf("Starting formal test of %d Compute operations...", testOps)
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	for range numCPU {
		go func() {
			for range testOps {
				mapIdx := runtime_cheaprand() & (numMaps - 1)
				key := int(runtime_cheaprand())

				maps[mapIdx].Store(key, key+1)

			}

			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgNs := elapsed.Nanoseconds() / testOps
	opsPerSec := float64(testOps) / elapsed.Seconds()

	t.Logf("Compute operation results:")
	t.Logf("  Total operations: %d", testOps)
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Average latency: %d ns/op", avgNs)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
	t.Logf("  Memory usage: %d MB", getMemUsage())

	t.Logf("Starting formal test of %d Load operations...", testOps)

	// Load test
	{
		time.Sleep(2 * time.Second)
		// force GC to ensure cold cache
		runtime.GC()
		runtime.GC()

		var wg sync.WaitGroup
		wg.Add(numCPU)

		start = time.Now()
		for range numCPU {
			go func() {
				for range testOps {
					mapIdx := runtime_cheaprand() & (numMaps - 1)
					key := int(runtime_cheaprand())
					_, _ = maps[mapIdx].Load(key)
				}

				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		avgNs := elapsed.Nanoseconds() / testOps
		opsPerSec := float64(testOps) / elapsed.Seconds()

		t.Logf("Load operation results:")
		t.Logf("  Total operations: %d", testOps)
		t.Logf("  Total time: %v", elapsed)
		t.Logf("  Average latency: %d ns/op", avgNs)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		t.Logf("  Memory usage: %d MB", getMemUsage())
	}
}

func TestColdStart_zhangyunhao116_skipmap(t *testing.T) {
	t.Logf("Initializing %d instances...", numMaps)

	// create array of 1 million map instance pointers
	maps := make([]*skipmap.OrderedMap[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = skipmap.New[int, int]()
	}

	// force GC to ensure cold cache
	runtime.GC()
	runtime.GC()

	t.Logf("Starting formal test of %d Compute operations...", testOps)
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	for range numCPU {
		go func() {
			for range testOps {
				mapIdx := runtime_cheaprand() & (numMaps - 1)
				key := int(runtime_cheaprand())

				maps[mapIdx].Store(key, key+1)

			}

			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgNs := elapsed.Nanoseconds() / testOps
	opsPerSec := float64(testOps) / elapsed.Seconds()

	t.Logf("Compute operation results:")
	t.Logf("  Total operations: %d", testOps)
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Average latency: %d ns/op", avgNs)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
	t.Logf("  Memory usage: %d MB", getMemUsage())

	t.Logf("Starting formal test of %d Load operations...", testOps)

	// Load test
	{
		time.Sleep(2 * time.Second)
		// force GC to ensure cold cache
		runtime.GC()
		runtime.GC()

		var wg sync.WaitGroup
		wg.Add(numCPU)

		start = time.Now()
		for range numCPU {
			go func() {
				for i := 0; i < testOps; i++ {
					mapIdx := runtime_cheaprand() & (numMaps - 1)
					key := int(runtime_cheaprand())
					_, _ = maps[mapIdx].Load(key)
				}

				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		avgNs := elapsed.Nanoseconds() / testOps
		opsPerSec := float64(testOps) / elapsed.Seconds()

		t.Logf("Load operation results:")
		t.Logf("  Total operations: %d", testOps)
		t.Logf("  Total time: %v", elapsed)
		t.Logf("  Average latency: %d ns/op", avgNs)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		t.Logf("  Memory usage: %d MB", getMemUsage())
	}
}

func TestColdStart_alphadose(t *testing.T) {
	t.Logf("Initializing %d instances...", numMaps)

	// create array of 1 million map instance pointers
	maps := make([]*haxmap.Map[int, int], numMaps)
	for i := range numMaps {
		maps[i] = haxmap.New[int, int]()
	}

	// force GC to ensure cold cache
	runtime.GC()
	runtime.GC()

	t.Logf("Starting formal test of %d Compute operations...", testOps)
	var wg sync.WaitGroup
	wg.Add(numCPU)

	start := time.Now()

	for range numCPU {
		go func() {
			for range testOps {
				mapIdx := runtime_cheaprand() & (numMaps - 1)
				key := int(runtime_cheaprand())

				maps[mapIdx].Set(key, key+1)

			}

			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	avgNs := elapsed.Nanoseconds() / testOps
	opsPerSec := float64(testOps) / elapsed.Seconds()

	t.Logf("Compute operation results:")
	t.Logf("  Total operations: %d", testOps)
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Average latency: %d ns/op", avgNs)
	t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
	t.Logf("  Memory usage: %d MB", getMemUsage())

	t.Logf("Starting formal test of %d Load operations...", testOps)

	// Load test
	{
		time.Sleep(2 * time.Second)
		// force GC to ensure cold cache
		runtime.GC()
		runtime.GC()

		var wg sync.WaitGroup
		wg.Add(numCPU)

		start = time.Now()
		for range numCPU {
			go func() {
				for i := 0; i < testOps; i++ {
					mapIdx := runtime_cheaprand() & (numMaps - 1)
					key := int(runtime_cheaprand())
					_, _ = maps[mapIdx].Get(key)
				}

				wg.Done()
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		avgNs := elapsed.Nanoseconds() / testOps
		opsPerSec := float64(testOps) / elapsed.Seconds()

		t.Logf("Load operation results:")
		t.Logf("  Total operations: %d", testOps)
		t.Logf("  Total time: %v", elapsed)
		t.Logf("  Average latency: %d ns/op", avgNs)
		t.Logf("  Throughput: %.0f ops/sec", opsPerSec)
		t.Logf("  Memory usage: %d MB", getMemUsage())
	}
}
