package benchmark

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jeremiah-masters/dlht"
	"github.com/llxisdsh/cc"
	"github.com/puzpuzpuz/xsync/v4"
)

type stableIntMap interface {
	Insert(k int, v int)
	Load(k int) (int, bool)
	Delete(k int)
	Size() int
}

type stableIntMapFactory struct {
	name string
	new  func(capHint int) stableIntMap
}
type stableIntSyncMapAdapter struct{ m *sync.Map }

func (a *stableIntSyncMapAdapter) Insert(k int, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableIntSyncMapAdapter) Load(k int) (int, bool) { v, ok := a.m.Load(k); return v.(int), ok }
func (a *stableIntSyncMapAdapter) Delete(k int)           { a.m.Delete(k) }
func (a *stableIntSyncMapAdapter) Size() int {
	size := 0
	a.m.Range(func(key, value any) bool { size++; return true })
	return size
}

type stableIntCCMapAdapter struct{ m *cc.Map[int, int] }

func (a *stableIntCCMapAdapter) Insert(k int, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableIntCCMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }
func (a *stableIntCCMapAdapter) Delete(k int)           { a.m.Delete(k) }
func (a *stableIntCCMapAdapter) Size() int              { return a.m.Size() }

type stableIntCCFlatMapAdapter struct{ m *cc.FlatMap[int, int] }

func (a *stableIntCCFlatMapAdapter) Insert(k int, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableIntCCFlatMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }
func (a *stableIntCCFlatMapAdapter) Delete(k int)           { a.m.Delete(k) }
func (a *stableIntCCFlatMapAdapter) Size() int              { return a.m.Size() }

type stableIntCCFunnelMapAdapter struct{ m *cc.FunnelMap[int, int] }

func (a *stableIntCCFunnelMapAdapter) Insert(k int, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableIntCCFunnelMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }
func (a *stableIntCCFunnelMapAdapter) Delete(k int)           { a.m.Delete(k) }
func (a *stableIntCCFunnelMapAdapter) Size() int              { return a.m.Size() }

type stableIntCCOFHTMapAdapter struct{ m *cc.OFHTMap[int, int] }

func (a *stableIntCCOFHTMapAdapter) Insert(k int, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableIntCCOFHTMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }
func (a *stableIntCCOFHTMapAdapter) Delete(k int)           { a.m.Delete(k) }
func (a *stableIntCCOFHTMapAdapter) Size() int              { return a.m.Size() }

type stableIntCCDWHTMapAdapter struct{ m *cc.DWHTMap[int, int] }

func (a *stableIntCCDWHTMapAdapter) Insert(k int, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableIntCCDWHTMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }
func (a *stableIntCCDWHTMapAdapter) Delete(k int)           { a.m.Delete(k) }
func (a *stableIntCCDWHTMapAdapter) Size() int              { return a.m.Size() }

type stableIntXsyncMapAdapter struct{ m *xsync.Map[int, int] }

func (a *stableIntXsyncMapAdapter) Insert(k int, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableIntXsyncMapAdapter) Load(k int) (int, bool) { return a.m.Load(k) }
func (a *stableIntXsyncMapAdapter) Delete(k int)           { a.m.Delete(k) }
func (a *stableIntXsyncMapAdapter) Size() int              { return a.m.Size() }

type stableIntDLHTMapAdapter struct{ m *dlht.Map[int, int] }

func (a *stableIntDLHTMapAdapter) Insert(k int, v int)    { a.m.Insert(k, v) }
func (a *stableIntDLHTMapAdapter) Load(k int) (int, bool) { return a.m.Get(k) }
func (a *stableIntDLHTMapAdapter) Delete(k int)           { a.m.Delete(k) }
func (a *stableIntDLHTMapAdapter) Size() int              { return int(a.m.Size()) }

// TestIntThroughputStable uses one shared harness for int-key throughput.
func TestIntThroughputStable(t *testing.T) {
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

	factories := []stableIntMapFactory{
		{
			name: "sync.Map",
			new: func(capHint int) stableIntMap {
				return &stableIntSyncMapAdapter{m: &sync.Map{}}
			},
		},
		{
			name: "cc.Map",
			new: func(capHint int) stableIntMap {
				return &stableIntCCMapAdapter{m: cc.NewMap[int, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "cc.FlatMap",
			new: func(capHint int) stableIntMap {
				return &stableIntCCFlatMapAdapter{m: cc.NewFlatMap[int, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "cc.FunnelMap",
			new: func(capHint int) stableIntMap {
				return &stableIntCCFunnelMapAdapter{m: cc.NewFunnelMap[int, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "cc.OFHTMap",
			new: func(capHint int) stableIntMap {
				return &stableIntCCOFHTMapAdapter{m: cc.NewOFHTMap[int, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "cc.DWHTMap",
			new: func(capHint int) stableIntMap {
				return &stableIntCCDWHTMapAdapter{m: cc.NewDWHTMap[int, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "xsync.Map",
			new: func(capHint int) stableIntMap {
				return &stableIntXsyncMapAdapter{m: xsync.NewMap[int, int](xsync.WithPresize(capHint))}
			},
		},
		{
			name: "dlht.Map",
			new: func(capHint int) stableIntMap {
				return &stableIntDLHTMapAdapter{m: dlht.New[int, int](dlht.Options{InitialSize: uint64(capHint)})}
			},
		},
	}

	modes := []struct {
		name   string
		preCap bool
	}{
		{name: "no_pre_size", preCap: false},
		{name: "pre_size", preCap: true},
	}
	type summaryRow struct {
		mode      string
		name      string
		insertMed float64
		loadMed   float64
		delMed    float64
		memMiB    float64
		memBPE    float64
		insertJit float64
		loadJit   float64
		delJit    float64
	}
	finalSummary := make([]summaryRow, 0, len(modes)*len(factories))

	for _, mode := range modes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			type mapSummary struct {
				insert   []float64
				load     []float64
				del      []float64
				memMiB   []float64
				memBytes []float64
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
							keys := make([]int, total)
							for i := range total {
								keys[i] = i
							}

							insertTP := make([]float64, 0, rounds)
							loadTP := make([]float64, 0, rounds)
							deleteTP := make([]float64, 0, rounds)
							memoryMiB := make([]float64, 0, rounds)
							memoryBPE := make([]float64, 0, rounds)
							capHint := 0
							if mode.preCap {
								capHint = total
							}

							for round := 0; round < rounds; round++ {
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
								var memAfter runtime.MemStats
								runtime.ReadMemStats(&memAfter)
								memBytes := allocDeltaBytes(memBefore.Alloc, memAfter.Alloc)
								memBytesPerEntry := memBytes / float64(total)

								loadDur := runParallel(total, workers, func(start, end int) {
									for i := start; i < end; i++ {
										v, ok := m.Load(keys[i])
										if !ok || v != i {
											t.Fatalf("load mismatch key=%d ok=%v v=%d", keys[i], ok, v)
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
								memMiB := memBytes / (1024 * 1024)
								memoryMiB = append(memoryMiB, memMiB)
								memoryBPE = append(memoryBPE, memBytesPerEntry)
								sum.memMiB = append(sum.memMiB, memMiB)
								sum.memBytes = append(sum.memBytes, memBytesPerEntry)
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
									"memory: median=%.2f MiB median=%.1f B/entry",
									median(memoryMiB), median(memoryBPE),
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
				if s == nil || len(s.insert) == 0 || len(s.load) == 0 || len(s.del) == 0 || len(s.memMiB) == 0 {
					continue
				}
				iMed, lMed, dMed := median(s.insert), median(s.load), median(s.del)
				mMed, bMed := median(s.memMiB), median(s.memBytes)
				iJit := slices.Max(s.insert) / max(slices.Min(s.insert), 1e-9)
				lJit := slices.Max(s.load) / max(slices.Min(s.load), 1e-9)
				dJit := slices.Max(s.del) / max(slices.Min(s.del), 1e-9)
				finalSummary = append(finalSummary, summaryRow{
					mode:      mode.name,
					name:      f.name,
					insertMed: iMed,
					loadMed:   lMed,
					delMed:    dMed,
					memMiB:    mMed,
					memBPE:    bMed,
					insertJit: iJit,
					loadJit:   lJit,
					delJit:    dJit,
				})
			}
		})
	}

	t.Log("===== final summary (sorted by insert median, high->low) =====")
	for _, mode := range modes {
		rows := make([]summaryRow, 0, len(factories))
		for _, r := range finalSummary {
			if r.mode == mode.name {
				rows = append(rows, r)
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].insertMed > rows[j].insertMed })
		t.Logf("mode=%s", mode.name)
		for _, r := range rows {
			t.Logf(
				"%s | insert=%.2f load=%.2f delete=%.2f mops | mem=%.1f B/entry %.2f MiB | jitter i/l/d=%.2fx/%.2fx/%.2fx",
				r.name, r.insertMed, r.loadMed, r.delMed, r.memBPE, r.memMiB, r.insertJit, r.loadJit, r.delJit,
			)
		}
	}
}
