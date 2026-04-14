package benchmark

import (
	"cmp"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/llxisdsh/cc"
	csmap "github.com/mhmtszr/concurrent-swiss-map"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/zhangyunhao116/skipmap"
)

// ============================================================================
// Global Configuration
// ============================================================================

// useBuiltInHasher controls whether to use cc.WithBuiltInHasher[int]()
// Set to true for faster int hashing, false for default behavior
var useBuiltInHasher = false

// Test parameters - adjust for stability vs speed tradeoff
const (
	defaultOpsPerWorker = 50000 // Operations per worker
	defaultKeys         = 10000 // Number of keys
	warmupRounds        = 3     // Warmup iterations before measurement
	measureRounds       = 5     // Measurement rounds to average
	batchSize           = 5     // measure latency per batch to overcome Windows timer precision (~15ms)
)

// Helper to create cc maps with optional built-in hasher
func newFlatMap() *cc.FlatMap[int, int] {
	//goland:noinspection GoBoolExpressions
	if useBuiltInHasher {
		return cc.NewFlatMap[int, int](cc.WithBuiltInHasher[int]())
	}
	return cc.NewFlatMap[int, int]()
}

func newMap() *cc.Map[int, int] {
	//goland:noinspection GoBoolExpressions
	if useBuiltInHasher {
		return cc.NewMap[int, int](cc.WithBuiltInHasher[int]())
	}
	return cc.NewMap[int, int]()
}

// ============================================================================
// Map Adapters
// ============================================================================

type MapInterface interface {
	Store(key, value int)
	Load(key int) (int, bool)
}

type flatMapAdapter struct{ m *cc.FlatMap[int, int] }

func (a *flatMapAdapter) Store(k, v int)         { a.m.Store(k, v) }
func (a *flatMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }

type mapAdapter struct{ m *cc.Map[int, int] }

func (a *mapAdapter) Store(k, v int)         { a.m.Store(k, v) }
func (a *mapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }

type xsyncMapAdapter struct{ m *xsync.Map[int, int] }

func (a *xsyncMapAdapter) Store(k, v int)         { a.m.Store(k, v) }
func (a *xsyncMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }

type syncMapAdapter struct{ m *sync.Map }

func (a *syncMapAdapter) Store(k, v int) { a.m.Store(k, v) }
func (a *syncMapAdapter) Load(k int) (int, bool) {
	v, ok := a.m.Load(k)
	if ok {
		return v.(int), true
	}
	return 0, false
}

type rwShardedMapAdapter struct{ m *RWLockShardedMap[int, int] }

func (a *rwShardedMapAdapter) Store(k, v int)         { a.m.Store(k, v) }
func (a *rwShardedMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }

type haxmapAdapter struct{ m *haxmap.Map[int, int] }

func (a *haxmapAdapter) Store(k, v int)         { a.m.Set(k, v) }
func (a *haxmapAdapter) Load(k int) (int, bool) { return a.m.Get(k) }

type csMapAdapter struct{ m *csmap.CsMap[int, int] }

func (a *csMapAdapter) Store(k, v int)         { a.m.Store(k, v) }
func (a *csMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }

type zhangyunhao116SkipmapAdapter struct {
	m *skipmap.OrderedMap[int, int]
}

func (a *zhangyunhao116SkipmapAdapter) Store(k, v int)         { a.m.Store(k, v) }
func (a *zhangyunhao116SkipmapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }

type ccFunnelMapAdapter struct {
	m *cc.FunnelMap[int, int]
}

func (a *ccFunnelMapAdapter) Store(k, v int)         { a.m.Store(k, v) }
func (a *ccFunnelMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }

type ccSkipMapAdapter struct {
	m *cc.SkipMap[int, int]
}

func (a *ccSkipMapAdapter) Store(k, v int)         { a.m.Store(k, v) }
func (a *ccSkipMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }

// ============================================================================
// Latency Result
// ============================================================================

type latencyResult struct {
	name       string
	throughput float64
	avg        time.Duration
	p50        time.Duration
	p99        time.Duration
	p999       time.Duration
	max        time.Duration
	slowRate   float64 // % of ops > 1ms
}

// ms formats duration as milliseconds with 2 decimal places
func ms(d time.Duration) string {
	return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/float64(time.Microsecond))
}

func runLatencyTest(workers, keys, opsPerWorker int, m MapInterface) latencyResult {
	batches := max(opsPerWorker/batchSize, 1)
	totalBatches := workers * batches
	samples := make([]int64, totalBatches)
	var sampleIdx atomic.Int64
	var slowOps atomic.Int64

	// Pre-populate
	for i := range keys {
		m.Store(i, i)
	}
	runtime.GC()

	var wg sync.WaitGroup
	start := time.Now()

	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			baseKey := workerID * opsPerWorker

			for b := range batches {
				batchStart := time.Now()

				// Run a batch of operations
				for i := range batchSize {
					key := (baseKey + b*batchSize + i) % keys
					if i%5 == 0 {
						m.Store(key, workerID)
					} else {
						_, _ = m.Load(key)
					}
				}

				batchLatency := time.Since(batchStart).Nanoseconds()
				perOpLatency := batchLatency / int64(batchSize) // Average per operation

				if perOpLatency > int64(time.Microsecond) {
					slowOps.Add(1)
				}

				idx := sampleIdx.Add(1) - 1
				if idx < int64(totalBatches) {
					samples[idx] = perOpLatency
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(samples)

	var sum int64
	for _, v := range samples {
		sum += v
	}

	totalOps := workers * batches * batchSize

	return latencyResult{
		throughput: float64(totalOps) / elapsed.Seconds(),
		avg:        time.Duration(sum / int64(len(samples))),
		p50:        time.Duration(samples[len(samples)/2]),
		p99:        time.Duration(samples[int(float64(len(samples))*0.99)]),
		p999:       time.Duration(samples[int(float64(len(samples)-1)*0.999)]),
		max:        time.Duration(samples[len(samples)-1]),
		slowRate:   float64(slowOps.Load()) / float64(totalOps) * 100,
	}
}

// runWithWarmup runs warmup rounds then measurement rounds and returns averaged result
func runWithWarmup(workers, keys, ops int, makeMap func() MapInterface) latencyResult {
	// Warmup
	for range warmupRounds {
		m := makeMap()
		_ = runLatencyTest(workers, keys, ops/10, m) // Smaller warmup
	}
	runtime.GC()
	time.Sleep(10 * time.Millisecond)

	// Measurement rounds
	var results []latencyResult
	for range measureRounds {
		m := makeMap()
		r := runLatencyTest(workers, keys, ops, m)
		results = append(results, r)
		runtime.GC()
	}

	// Average results
	var avgResult latencyResult
	for _, r := range results {
		avgResult.throughput += r.throughput
		avgResult.avg += r.avg
		avgResult.p50 += r.p50
		avgResult.p99 += r.p99
		avgResult.p999 += r.p999
		avgResult.max += r.max
		avgResult.slowRate += r.slowRate
	}
	n := float64(len(results))
	avgResult.throughput /= n
	avgResult.avg /= time.Duration(n)
	avgResult.p50 /= time.Duration(n)
	avgResult.p99 /= time.Duration(n)
	avgResult.p999 /= time.Duration(n)
	avgResult.max /= time.Duration(n)
	avgResult.slowRate /= n

	return avgResult
}

// ============================================================================
// Main Test
// ============================================================================

func TestLatencySummary(t *testing.T) {
	numCPU := runtime.GOMAXPROCS(0)
	workers := numCPU * 4
	keys := defaultKeys
	ops := defaultOpsPerWorker

	t.Logf("=== Tail Latency Benchmark ===")
	t.Logf("CPUs: %d, Workers: %d, Keys: %d, Ops/Worker: %d", numCPU, workers, keys, ops)

	//goland:noinspection GoBoolExpressions
	t.Logf("Warmup: %d rounds, Measure: %d rounds, useBuiltInHasher: %v\n",
		warmupRounds, measureRounds, useBuiltInHasher)

	impls := []struct {
		name string
		make func() MapInterface
	}{
		{"sync.Map", func() MapInterface { return &syncMapAdapter{&sync.Map{}} }},
		{"RWShardedMap", func() MapInterface { return &rwShardedMapAdapter{NewRWLockShardedMap[int, int](numCPU * 4)} }},
		{"xsync.Map", func() MapInterface { return &xsyncMapAdapter{xsync.NewMap[int, int]()} }},
		{"cc.Map", func() MapInterface { return &mapAdapter{newMap()} }},
		{"cc.FlatMap", func() MapInterface { return &flatMapAdapter{newFlatMap()} }},
		{"alphadose.haxmap", func() MapInterface { return &haxmapAdapter{haxmap.New[int, int]()} }},
		{"mhmtszr.csmap", func() MapInterface { return &csMapAdapter{csmap.New[int, int]()} }},
		{"zhangyunhao116.skipmap", func() MapInterface { return &zhangyunhao116SkipmapAdapter{skipmap.New[int, int]()} }},
		{"cc.SkipMap", func() MapInterface { return &ccSkipMapAdapter{cc.NewSkipMap[int, int]()} }},
		{"cc.FunnelMap", func() MapInterface { return &ccFunnelMapAdapter{cc.NewFunnelMap[int, int]()} }},
	}

	var results []*latencyResult
	for _, impl := range impls {
		t.Logf("Testing %s...", impl.name)
		r := runWithWarmup(workers, keys, ops, impl.make)
		r.name = impl.name
		results = append(results, &r)
	}

	// Sort by slowRate (lower is better)

	slices.SortFunc(results, func(a, b *latencyResult) int {
		return cmp.Compare(a.slowRate, b.slowRate)
	})

	t.Log("\n=== Results (sorted by slowRate) ===")
	t.Logf("%-4s | %-22s | %12s | %10s | %10s | %10s | %8s",
		"Rank", "Implementation", "Throughput", "p99", "p999", "max", "(>1µs)%")
	t.Logf("-----|------------------------|--------------|------------|------------|------------|----------")
	for i, r := range results {
		t.Logf("%-4d | %-22s | %10.0f/s | %10s | %10s | %10s | %6.4f%%",
			i+1, r.name, r.throughput, ms(r.p99), ms(r.p999), ms(r.max), r.slowRate)
	}

	t.Logf("✓ Best slowRate(>1µs)%%: %s (%6.4f%%)", results[0].name, results[0].slowRate)

	// Find best throughput
	best := results[0]
	for _, r := range results {
		if r.throughput > best.throughput {
			best = r
		}
	}
	t.Logf("✓ Best throughput: %s (%.0f/s)", best.name, best.throughput)
}
