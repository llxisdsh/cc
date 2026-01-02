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

// getMemUsage 获取当前内存使用量(MB)
func getMemUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc / 1024 / 1024
}

const (
	numMaps = 1024 * 1024 // map输得, 必须2的幂
	testOps = 10_000_000  // 测试操作次数
	numCPU  = 8
)

func TestColdStart_cc_FlatMap(t *testing.T) {
	t.Logf("初始化 %d 个实例...", numMaps)

	// 创建100万个map实例的指针数组
	maps := make([]*cc.FlatMap[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = cc.NewFlatMap[int, int]()
	}

	// 强制GC，确保冷缓存
	runtime.GC()
	runtime.GC()

	t.Logf("开始正式测试 %d 次 Compute 操作...", testOps)
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

	t.Logf("Compute 操作结果:")
	t.Logf("  总操作数: %d", testOps)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  平均延迟: %d ns/op", avgNs)
	t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
	t.Logf("  内存使用: %d MB", getMemUsage())

	t.Logf("开始正式测试 %d 次 Load 操作...", testOps)

	// Load测试
	{
		time.Sleep(2 * time.Second)
		// 强制GC，确保冷缓存
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

		t.Logf("Load 操作结果:")
		t.Logf("  总操作数: %d", testOps)
		t.Logf("  总耗时: %v", elapsed)
		t.Logf("  平均延迟: %d ns/op", avgNs)
		t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
		t.Logf("  内存使用: %d MB", getMemUsage())
	}
}

func TestColdStart_cc_Map(t *testing.T) {
	t.Logf("初始化 %d 个实例...", numMaps)

	// 创建100万个map实例的指针数组
	maps := make([]*cc.Map[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = cc.NewMap[int, int]()
	}

	// 强制GC，确保冷缓存
	runtime.GC()
	runtime.GC()

	t.Logf("开始正式测试 %d 次 Compute 操作...", testOps)
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

	t.Logf("Compute 操作结果:")
	t.Logf("  总操作数: %d", testOps)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  平均延迟: %d ns/op", avgNs)
	t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
	t.Logf("  内存使用: %d MB", getMemUsage())

	t.Logf("开始正式测试 %d 次 Load 操作...", testOps)

	// Load测试
	{
		time.Sleep(2 * time.Second)
		// 强制GC，确保冷缓存
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

		t.Logf("Load 操作结果:")
		t.Logf("  总操作数: %d", testOps)
		t.Logf("  总耗时: %v", elapsed)
		t.Logf("  平均延迟: %d ns/op", avgNs)
		t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
		t.Logf("  内存使用: %d MB", getMemUsage())
	}
}

func TestColdStart_RWLockShardedMap(t *testing.T) {
	t.Logf("初始化 %d 个实例...", numMaps)

	// 创建100万个map实例的指针数组
	maps := make([]*RWLockShardedMap[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = NewRWLockShardedMap[int, int](256)
	}

	// 强制GC，确保冷缓存
	runtime.GC()
	runtime.GC()

	t.Logf("开始正式测试 %d 次 Compute 操作...", testOps)
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

	t.Logf("Compute 操作结果:")
	t.Logf("  总操作数: %d", testOps)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  平均延迟: %d ns/op", avgNs)
	t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
	t.Logf("  内存使用: %d MB", getMemUsage())

	t.Logf("开始正式测试 %d 次 Load 操作...", testOps)

	// Load测试
	{
		time.Sleep(2 * time.Second)
		// 强制GC，确保冷缓存
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

		t.Logf("Load 操作结果:")
		t.Logf("  总操作数: %d", testOps)
		t.Logf("  总耗时: %v", elapsed)
		t.Logf("  平均延迟: %d ns/op", avgNs)
		t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
		t.Logf("  内存使用: %d MB", getMemUsage())
	}
}

func TestColdStart_xsync_Map(t *testing.T) {
	t.Logf("初始化 %d 个实例...", numMaps)

	// 创建100万个map实例的指针数组
	maps := make([]*xsync.Map[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = xsync.NewMap[int, int]()
	}

	// 强制GC，确保冷缓存
	runtime.GC()
	runtime.GC()

	t.Logf("开始正式测试 %d 次 Compute 操作...", testOps)
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

	t.Logf("Compute 操作结果:")
	t.Logf("  总操作数: %d", testOps)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  平均延迟: %d ns/op", avgNs)
	t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
	t.Logf("  内存使用: %d MB", getMemUsage())

	t.Logf("开始正式测试 %d 次 Load 操作...", testOps)

	// Load测试
	{
		time.Sleep(2 * time.Second)
		// 强制GC，确保冷缓存
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

		t.Logf("Load 操作结果:")
		t.Logf("  总操作数: %d", testOps)
		t.Logf("  总耗时: %v", elapsed)
		t.Logf("  平均延迟: %d ns/op", avgNs)
		t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
		t.Logf("  内存使用: %d MB", getMemUsage())
	}
}

func TestColdStart_sync_Map(t *testing.T) {
	t.Logf("初始化 %d 个实例...", numMaps)

	// 创建100万个map实例的指针数组
	maps := make([]*sync.Map, numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = &sync.Map{}
	}

	// 强制GC，确保冷缓存
	runtime.GC()
	runtime.GC()

	t.Logf("开始正式测试 %d 次 Compute 操作...", testOps)
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

	t.Logf("Compute 操作结果:")
	t.Logf("  总操作数: %d", testOps)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  平均延迟: %d ns/op", avgNs)
	t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
	t.Logf("  内存使用: %d MB", getMemUsage())

	t.Logf("开始正式测试 %d 次 Load 操作...", testOps)

	// Load测试
	{
		time.Sleep(2 * time.Second)
		// 强制GC，确保冷缓存
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

		t.Logf("Load 操作结果:")
		t.Logf("  总操作数: %d", testOps)
		t.Logf("  总耗时: %v", elapsed)
		t.Logf("  平均延迟: %d ns/op", avgNs)
		t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
		t.Logf("  内存使用: %d MB", getMemUsage())
	}
}

func TestColdStart_zhangyunhao116_skipmap(t *testing.T) {
	t.Logf("初始化 %d 个实例...", numMaps)

	// 创建100万个map实例的指针数组
	maps := make([]*skipmap.OrderedMap[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = skipmap.New[int, int]()
	}

	// 强制GC，确保冷缓存
	runtime.GC()
	runtime.GC()

	t.Logf("开始正式测试 %d 次 Compute 操作...", testOps)
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

	t.Logf("Compute 操作结果:")
	t.Logf("  总操作数: %d", testOps)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  平均延迟: %d ns/op", avgNs)
	t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
	t.Logf("  内存使用: %d MB", getMemUsage())

	t.Logf("开始正式测试 %d 次 Load 操作...", testOps)

	// Load测试
	{
		time.Sleep(2 * time.Second)
		// 强制GC，确保冷缓存
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

		t.Logf("Load 操作结果:")
		t.Logf("  总操作数: %d", testOps)
		t.Logf("  总耗时: %v", elapsed)
		t.Logf("  平均延迟: %d ns/op", avgNs)
		t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
		t.Logf("  内存使用: %d MB", getMemUsage())
	}
}

func TestColdStart_alphadose(t *testing.T) {
	t.Logf("初始化 %d 个实例...", numMaps)

	// 创建100万个map实例的指针数组
	maps := make([]*haxmap.Map[int, int], numMaps)
	for i := 0; i < numMaps; i++ {
		maps[i] = haxmap.New[int, int]()
	}

	// 强制GC，确保冷缓存
	runtime.GC()
	runtime.GC()

	t.Logf("开始正式测试 %d 次 Compute 操作...", testOps)
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

	t.Logf("Compute 操作结果:")
	t.Logf("  总操作数: %d", testOps)
	t.Logf("  总耗时: %v", elapsed)
	t.Logf("  平均延迟: %d ns/op", avgNs)
	t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
	t.Logf("  内存使用: %d MB", getMemUsage())

	t.Logf("开始正式测试 %d 次 Load 操作...", testOps)

	// Load测试
	{
		time.Sleep(2 * time.Second)
		// 强制GC，确保冷缓存
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

		t.Logf("Load 操作结果:")
		t.Logf("  总操作数: %d", testOps)
		t.Logf("  总耗时: %v", elapsed)
		t.Logf("  平均延迟: %d ns/op", avgNs)
		t.Logf("  吞吐量: %.0f ops/sec", opsPerSec)
		t.Logf("  内存使用: %d MB", getMemUsage())
	}
}
