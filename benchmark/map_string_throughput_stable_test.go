package benchmark

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/llxisdsh/cc"
	"github.com/llxisdsh/pb"
	"github.com/puzpuzpuz/xsync/v4"
)

// TestStringThroughputStable uses one shared harness for string-key throughput.
func TestStringThroughputStable(t *testing.T) {
	workers := runtime.GOMAXPROCS(0)
	rounds := 3
	minMeasure := 1 * time.Microsecond
	maxPerMapMode := 8 * time.Second
	base := 262_144
	maxCenter := 4_194_304 // 2^22
	centerCount := 5
	if testing.Short() {
		maxPerMapMode = 2 * time.Second
		maxCenter = 1_048_576 // 2^20
		centerCount = 3
	}
	ratios := []float64{0.90, 1.00, 1.10}
	scales := buildLadderScales(base, maxCenter, centerCount, ratios)
	detail := os.Getenv("CC_THROUGHPUT_DETAIL") == "1"

	factories := []stableFactory[string]{
		stableSyncMapFactory[string]("sync.Map"),
		stableLoadOrStoreFactory("pb.HashTrieMap", func(capHint int) *pb.HashTrieMap[string, int] {
			return &pb.HashTrieMap[string, int]{}
		}),
		stableLoadOrStoreFactory("xsync.Map", func(capHint int) *xsync.Map[string, int] {
			return xsync.NewMap[string, int](xsync.WithPresize(capHint))
		}),
		stableCSMapFactory[string]("concurrent-swiss-map"),
		stableDLHTFactory[string]("dlht.Map"),
		stableLoadOrStoreFactory("cc.Map", func(capHint int) *cc.Map[string, int] {
			return cc.NewMap[string, int](cc.WithCapacity(capHint))
		}),
		stableLoadOrStoreFactory("cc.FlatMap", func(capHint int) *cc.FlatMap[string, int] {
			return cc.NewFlatMap[string, int](cc.WithCapacity(capHint))
		}),
		stableLoadOrStoreFactory("cc.FunnelMap", func(capHint int) *cc.FunnelMap[string, int] {
			return cc.NewFunnelMap[string, int](cc.WithCapacity(capHint))
		}),
		stableLoadOrStoreFactory("cc.SkipMap", func(capHint int) *cc.SkipMap[string, int] {
			return cc.NewSkipMap[string, int]()
		}),
		stableLoadOrStoreFactory("cc.OFHTMap", func(capHint int) *cc.OFHTMap[string, int] {
			return cc.NewOFHTMap[string, int](cc.WithCapacity(capHint))
		}),
		stableLoadOrStoreFactory("cc.DWHTMap", func(capHint int) *cc.DWHTMap[string, int] {
			return cc.NewDWHTMap[string, int](cc.WithCapacity(capHint))
		}),
		// // Requires goexperiment.simd
		// stableLoadOrStoreFactory("cc.V28Map", func(capHint int) *cc.V28Map[string, int] {
		// 	return cc.NewV28Map[string, int](cc.WithCapacity(capHint))
		// }),
		stableLoadOrStoreFactory("cc.V6Map", func(capHint int) *cc.V6Map[string, int] {
			return cc.NewV6Map[string, int](cc.WithCapacity(capHint))
		}),
	}

	modes := []struct {
		name   string
		preCap bool
	}{
		{name: "no_pre_size", preCap: false},
		{name: "pre_size", preCap: true},
	}
	type summaryRow struct {
		mode         string
		name         string
		insertMops   float64
		loadMops     float64
		delMops      float64
		retainedMiB  float64
		retainedBPE  float64
		allocatedMiB float64
		allocatedBPE float64
		memoryN      int64
		insertMin    float64
		insertMax    float64
		loadMin      float64
		loadMax      float64
		delMin       float64
		delMax       float64
	}
	finalSummary := make([]summaryRow, 0, len(modes)*len(factories))

	for _, mode := range modes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			type mapSummary struct {
				insert      []float64
				load        []float64
				del         []float64
				insertTotal stableThroughputTotal
				loadTotal   stableThroughputTotal
				delTotal    stableThroughputTotal
				memory      stableMemoryTotal
			}
			modeSummary := make(map[string]*mapSummary, len(factories))
			for _, f := range factories {
				f := f
				t.Run(f.name, func(t *testing.T) {
					startMode := time.Now()
					sum := &mapSummary{}
					modeSummary[f.name] = sum
					for _, total := range scales {
						if time.Since(startMode) > maxPerMapMode {
							t.Logf("stop early due to budget (%v)", maxPerMapMode)
							break
						}

						t.Run(fmt.Sprintf("n=%d", total), func(t *testing.T) {
							keys := make([]string, total)
							for i := range total {
								keys[i] = strconv.Itoa(i)
							}

							insertTP := make([]float64, 0, rounds)
							loadTP := make([]float64, 0, rounds)
							deleteTP := make([]float64, 0, rounds)
							var memory stableMemoryTotal
							capHint := 0
							if mode.preCap {
								capHint = total
							}

							for range rounds {
								runtime.GC()
								var memBefore runtime.MemStats
								runtime.ReadMemStats(&memBefore)
								m := f.new(capHint)

								insertDur := runParallel(total, workers, func(start, end int) {
									for i := start; i < end; i++ {
										m.Insert(keys[i], i)
									}
								})
								if got := m.Size(); got != total {
									t.Fatalf("insert size mismatch: want=%d got=%d", total, got)
								}
								var memAfterInsert runtime.MemStats
								runtime.ReadMemStats(&memAfterInsert)
								allocatedBytes := allocDeltaBytes(memBefore.TotalAlloc, memAfterInsert.TotalAlloc)

								runtime.GC()
								var memAfterGC runtime.MemStats
								runtime.ReadMemStats(&memAfterGC)
								retainedBytes := allocDeltaBytes(memBefore.Alloc, memAfterGC.Alloc)

								loadDur := runParallel(total, workers, func(start, end int) {
									for i := start; i < end; i++ {
										v, ok := m.Load(keys[i])
										if !ok || v != i {
											t.Fatalf("load mismatch key=%d ok=%v v=%d", i, ok, v)
										}
									}
								})

								deleteDur := runParallel(total, workers, func(start, end int) {
									for i := start; i < end; i++ {
										m.Delete(keys[i])
									}
								})
								if got := m.Size(); got != 0 {
									t.Fatalf("delete size mismatch: want=0 got=%d", got)
								}

								insertTP = append(insertTP, throughputMops(total, insertDur, minMeasure))
								loadTP = append(loadTP, throughputMops(total, loadDur, minMeasure))
								deleteTP = append(deleteTP, throughputMops(total, deleteDur, minMeasure))
								sum.insertTotal.add(total, insertDur, minMeasure)
								sum.loadTotal.add(total, loadDur, minMeasure)
								sum.delTotal.add(total, deleteDur, minMeasure)
								memory.add(total, retainedBytes, allocatedBytes)
								sum.memory.add(total, retainedBytes, allocatedBytes)
							}

							if detail {
								t.Logf(
									"insert mops: median=%.2f min=%.2f max=%.2f",
									median(insertTP), slices.Min(insertTP), slices.Max(insertTP),
								)
								t.Logf(
									"load   mops: median=%.2f min=%.2f max=%.2f",
									median(loadTP), slices.Min(loadTP), slices.Max(loadTP),
								)
								t.Logf(
									"delete mops: median=%.2f min=%.2f max=%.2f",
									median(deleteTP), slices.Min(deleteTP), slices.Max(deleteTP),
								)
								t.Logf(
									"retained total: %.2f MiB (%.1f B/entry, n=%d)",
									memory.retainedMiB(), memory.retainedBPE(), memory.entries,
								)
								t.Logf(
									"allocated total: %.2f MiB (%.1f B/entry, n=%d)",
									memory.allocatedMiB(), memory.allocatedBPE(), memory.entries,
								)
							}

							sum.insert = append(sum.insert, median(insertTP))
							sum.load = append(sum.load, median(loadTP))
							sum.del = append(sum.del, median(deleteTP))
						})
					}
				})
			}

			for _, f := range factories {
				s := modeSummary[f.name]
				if s == nil || len(s.insert) == 0 || len(s.load) == 0 || len(s.del) == 0 || s.memory.entries == 0 {
					continue
				}
				finalSummary = append(finalSummary, summaryRow{
					mode:         mode.name,
					name:         f.name,
					insertMops:   s.insertTotal.mops(),
					loadMops:     s.loadTotal.mops(),
					delMops:      s.delTotal.mops(),
					retainedMiB:  s.memory.retainedMiB(),
					retainedBPE:  s.memory.retainedBPE(),
					allocatedMiB: s.memory.allocatedMiB(),
					allocatedBPE: s.memory.allocatedBPE(),
					memoryN:      s.memory.entries,
					insertMin:    slices.Min(s.insert),
					insertMax:    slices.Max(s.insert),
					loadMin:      slices.Min(s.load),
					loadMax:      slices.Max(s.load),
					delMin:       slices.Min(s.del),
					delMax:       slices.Max(s.del),
				})
			}
		})
	}

	for _, mode := range modes {
		rows := make([]summaryRow, 0, len(factories))
		for _, r := range finalSummary {
			if r.mode == mode.name {
				rows = append(rows, r)
			}
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].memoryN > rows[j].memoryN {
				return true
			} else if rows[i].memoryN < rows[j].memoryN {
				return false
			}
			return rows[i].insertMops > rows[j].insertMops
		})
		fmt.Printf("\n===== mode %s final summary (total-based, sorted by insert throughput) =====\n\n", mode.name)
		for _, r := range rows {
			fmt.Printf(
				"%s(total n=%d) \n throughput(mops): insert=%.2f [%.2f..%.2f], load=%.2f [%.2f..%.2f], delete=%.2f [%.2f..%.2f] \n memory: retained=%.2f MiB (%.1f B/entry), allocated=%.2f MiB (%.1f B/entry)\n",
				r.name, r.memoryN,
				r.insertMops, r.insertMin, r.insertMax,
				r.loadMops, r.loadMin, r.loadMax,
				r.delMops, r.delMin, r.delMax,
				r.retainedMiB, r.retainedBPE,
				r.allocatedMiB, r.allocatedBPE,
			)
		}
	}
}

func buildLadderScales(base, maxCenter, centerCount int, ratios []float64) []int {
	seen := make(map[int]struct{}, centerCount*len(ratios))
	out := make([]int, 0, centerCount*len(ratios))
	for k := range centerCount {
		center := base << k
		if center > maxCenter {
			break
		}
		for _, r := range ratios {
			n := int(float64(center) * r)
			if n < 1 {
				n = 1
			}
			if _, ok := seen[n]; ok {
				continue
			}
			seen[n] = struct{}{}
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}

func runParallel(total, workers int, fn func(start, end int)) time.Duration {
	var wg sync.WaitGroup
	wg.Add(workers)
	batch := (total + workers - 1) / workers

	start := time.Now()
	for i := range workers {
		i := i
		go func() {
			s := i * batch
			e := min((i+1)*batch, total)
			if s < e {
				fn(s, e)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return time.Since(start)
}

func throughputMops(total int, d, minDur time.Duration) float64 {
	if d < minDur {
		d = minDur
	}
	sec := d.Seconds()
	if sec <= 0 {
		return 0
	}
	v := float64(total) / sec / 1_000_000
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return 0
	}
	return v
}

func allocDeltaBytes(before, after uint64) float64 {
	if after <= before {
		return 0
	}
	return float64(after - before)
}

type stableMemoryTotal struct {
	entries        int64
	retainedBytes  float64
	allocatedBytes float64
}

func (m *stableMemoryTotal) add(entries int, retainedBytes, allocatedBytes float64) {
	m.entries += int64(entries)
	m.retainedBytes += retainedBytes
	m.allocatedBytes += allocatedBytes
}

func (m stableMemoryTotal) retainedMiB() float64 {
	return m.retainedBytes / (1024 * 1024)
}

func (m stableMemoryTotal) retainedBPE() float64 {
	if m.entries == 0 {
		return 0
	}
	return m.retainedBytes / float64(m.entries)
}

func (m stableMemoryTotal) allocatedMiB() float64 {
	return m.allocatedBytes / (1024 * 1024)
}

func (m stableMemoryTotal) allocatedBPE() float64 {
	if m.entries == 0 {
		return 0
	}
	return m.allocatedBytes / float64(m.entries)
}

type stableThroughputTotal struct {
	entries int64
	elapsed time.Duration
}

func (t *stableThroughputTotal) add(entries int, elapsed, minDur time.Duration) {
	if elapsed < minDur {
		elapsed = minDur
	}
	t.entries += int64(entries)
	t.elapsed += elapsed
}

func (t stableThroughputTotal) mops() float64 {
	sec := t.elapsed.Seconds()
	if sec <= 0 {
		return 0
	}
	v := float64(t.entries) / sec / 1_000_000
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return 0
	}
	return v
}

func median(v []float64) float64 {
	cp := slices.Clone(v)
	slices.Sort(cp)
	m := len(cp) / 2
	if len(cp)%2 == 0 {
		return (cp[m-1] + cp[m]) / 2
	}
	return cp[m]
}
