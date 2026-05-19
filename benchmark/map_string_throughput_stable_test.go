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

	// "github.com/jeremiah-masters/dlht"
	"github.com/llxisdsh/cc"
	"github.com/puzpuzpuz/xsync/v4"
)

type stableMap interface {
	Insert(k string, v int)
	Load(k string) (int, bool)
	Delete(k string)
	Size() int
}

type stableMapFactory struct {
	name string
	new  func(capHint int) stableMap
}
type stableSyncMapAdapter struct{ m *sync.Map }

func (a *stableSyncMapAdapter) Insert(k string, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableSyncMapAdapter) Load(k string) (int, bool) { v, ok := a.m.Load(k); return v.(int), ok }
func (a *stableSyncMapAdapter) Delete(k string)           { a.m.Delete(k) }
func (a *stableSyncMapAdapter) Size() int {
	size := 0
	a.m.Range(func(key, value any) bool { size++; return true })
	return size
}

type stableCCMapAdapter struct{ m *cc.Map[string, int] }

func (a *stableCCMapAdapter) Insert(k string, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableCCMapAdapter) Load(k string) (int, bool) { return a.m.Load(k) }
func (a *stableCCMapAdapter) Delete(k string)           { a.m.Delete(k) }
func (a *stableCCMapAdapter) Size() int                 { return a.m.Size() }

type stableCCFlatMapAdapter struct{ m *cc.FlatMap[string, int] }

func (a *stableCCFlatMapAdapter) Insert(k string, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableCCFlatMapAdapter) Load(k string) (int, bool) { return a.m.Load(k) }
func (a *stableCCFlatMapAdapter) Delete(k string)           { a.m.Delete(k) }
func (a *stableCCFlatMapAdapter) Size() int                 { return a.m.Size() }

type stableCCOFHTMapAdapter struct{ m *cc.OFHTMap[string, int] }

func (a *stableCCOFHTMapAdapter) Insert(k string, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableCCOFHTMapAdapter) Load(k string) (int, bool) { return a.m.Load(k) }
func (a *stableCCOFHTMapAdapter) Delete(k string)           { a.m.Delete(k) }
func (a *stableCCOFHTMapAdapter) Size() int                 { return a.m.Size() }

type stableCCDWHTMapAdapter struct{ m *cc.DWHTMap[string, int] }

func (a *stableCCDWHTMapAdapter) Insert(k string, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableCCDWHTMapAdapter) Load(k string) (int, bool) { return a.m.Load(k) }
func (a *stableCCDWHTMapAdapter) Delete(k string)           { a.m.Delete(k) }
func (a *stableCCDWHTMapAdapter) Size() int                 { return a.m.Size() }

type stableXsyncMapAdapter struct{ m *xsync.Map[string, int] }

func (a *stableXsyncMapAdapter) Insert(k string, v int)    { a.m.LoadOrStore(k, v) }
func (a *stableXsyncMapAdapter) Load(k string) (int, bool) { return a.m.Load(k) }
func (a *stableXsyncMapAdapter) Delete(k string)           { a.m.Delete(k) }
func (a *stableXsyncMapAdapter) Size() int                 { return a.m.Size() }

// type stableDLHTMapAdapter struct{ m *dlht.Map[string, int] }
//
// func (a *stableDLHTMapAdapter) Insert(k string, v int)    { a.m.Insert(k, v) }
// func (a *stableDLHTMapAdapter) Load(k string) (int, bool) { return a.m.Get(k) }
// func (a *stableDLHTMapAdapter) Delete(k string)           { a.m.Delete(k) }
// func (a *stableDLHTMapAdapter) Size() int                 { return int(a.m.Size()) }

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

	factories := []stableMapFactory{
		{
			name: "sync.Map",
			new: func(capHint int) stableMap {
				return &stableSyncMapAdapter{m: &sync.Map{}}
			},
		},
		{
			name: "cc.Map",
			new: func(capHint int) stableMap {
				return &stableCCMapAdapter{m: cc.NewMap[string, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "cc.FlatMap",
			new: func(capHint int) stableMap {
				return &stableCCFlatMapAdapter{m: cc.NewFlatMap[string, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "cc.OFHTMap",
			new: func(capHint int) stableMap {
				return &stableCCOFHTMapAdapter{m: cc.NewOFHTMap[string, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "cc.DWHTMap",
			new: func(capHint int) stableMap {
				return &stableCCDWHTMapAdapter{m: cc.NewDWHTMap[string, int](cc.WithCapacity(capHint))}
			},
		},
		{
			name: "xsync.Map",
			new: func(capHint int) stableMap {
				return &stableXsyncMapAdapter{m: xsync.NewMap[string, int](xsync.WithPresize(capHint))}
			},
		},

		// {
		// 	name: "dlht.Map",
		// 	new: func(capHint int) stableMap {
		// 		return &stableDLHTMapAdapter{m: dlht.New[string, int](dlht.Options{InitialSize: uint64(capHint)})}
		// 	},
		// },
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
		insertJit float64
		loadJit   float64
		delJit    float64
	}
	finalSummary := make([]summaryRow, 0, len(modes)*len(factories))

	for _, mode := range modes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			type mapSummary struct {
				insert []float64
				load   []float64
				del    []float64
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
							capHint := 0
							if mode.preCap {
								capHint = total
							}

							for round := 0; round < rounds; round++ {
								runtime.GC()
								m := f.new(capHint)

								insertDur := runParallel(total, workers, func(start, end int) {
									for i := start; i < end; i++ {
										m.Insert(keys[i], i)
									}
								})
								if got := m.Size(); got != total {
									t.Fatalf("insert size mismatch: want=%d got=%d", total, got)
								}

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
				if s == nil || len(s.insert) == 0 || len(s.load) == 0 || len(s.del) == 0 {
					continue
				}
				iMed, lMed, dMed := median(s.insert), median(s.load), median(s.del)
				iJit := slices.Max(s.insert) / max(slices.Min(s.insert), 1e-9)
				lJit := slices.Max(s.load) / max(slices.Min(s.load), 1e-9)
				dJit := slices.Max(s.del) / max(slices.Min(s.del), 1e-9)
				finalSummary = append(finalSummary, summaryRow{
					mode:      mode.name,
					name:      f.name,
					insertMed: iMed,
					loadMed:   lMed,
					delMed:    dMed,
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
				"%s | insert=%.2f load=%.2f delete=%.2f mops | jitter i/l/d=%.2fx/%.2fx/%.2fx",
				r.name, r.insertMed, r.loadMed, r.delMed, r.insertJit, r.loadJit, r.delJit,
			)
		}
	}
}

func buildLadderScales(base, maxCenter, centerCount int, ratios []float64) []int {
	seen := make(map[int]struct{}, centerCount*len(ratios))
	out := make([]int, 0, centerCount*len(ratios))
	for k := 0; k < centerCount; k++ {
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
			defer wg.Done()
			s := i * batch
			e := min((i+1)*batch, total)
			if s < e {
				fn(s, e)
			}
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

func median(v []float64) float64 {
	cp := slices.Clone(v)
	slices.Sort(cp)
	m := len(cp) / 2
	if len(cp)%2 == 0 {
		return (cp[m-1] + cp[m]) / 2
	}
	return cp[m]
}
