//go:build !race

package cc

import (
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestV6MapZeroValue(t *testing.T) {
	var m V6Map[string, int]

	if _, ok := m.Load("missing"); ok {
		t.Fatal("zero-value Load hit")
	}
	if got := m.Size(); got != 0 {
		t.Fatalf("zero-value Size = %d, want 0", got)
	}
	called := false
	m.Range(func(string, int) bool {
		called = true
		return true
	})
	if called {
		t.Fatal("zero-value Range yielded an entry")
	}
	m.Delete("missing")
	if prev, ok := m.LoadAndUpdate("missing", 1); ok || prev != 0 {
		t.Fatalf("zero-value LoadAndUpdate = (%d, %v), want (0, false)", prev, ok)
	}
	if prev, ok := m.LoadAndDelete("missing"); ok || prev != 0 {
		t.Fatalf("zero-value LoadAndDelete = (%d, %v), want (0, false)", prev, ok)
	}
	if m.CompareAndSwap("missing", 0, 1) {
		t.Fatal("zero-value CompareAndSwap succeeded")
	}
	if m.CompareAndDelete("missing", 0) {
		t.Fatal("zero-value CompareAndDelete succeeded")
	}
	m.Clear()

	m.Store("a", 1)
	if v, ok := m.Load("a"); !ok || v != 1 {
		t.Fatalf("zero-value Store/Load = (%d, %v), want (1, true)", v, ok)
	}
	if !m.CompareAndSwap("a", 1, 2) {
		t.Fatal("CompareAndSwap after zero-value Store failed")
	}
	m.Clear()
	if _, ok := m.Load("a"); ok {
		t.Fatal("Clear did not remove zero-value initialized key")
	}

	var c V6Map[string, int]
	actual, loaded := c.Compute("k", func(e *MapEntry[string, int]) {
		if e.Loaded() {
			t.Fatal("zero-value Compute insert reported loaded")
		}
		e.Update(3)
	})
	if loaded || actual != 3 {
		t.Fatalf("zero-value Compute = (%d, %v), want (3, false)", actual, loaded)
	}
}

func TestV6MapPublicABAStringCrashHarness(t *testing.T) {
	if os.Getenv("CC_V6_PUBLIC_ABA_STRING_CRASH_CHILD") == "1" {
		runV6MapPublicABAStringCrashChild()
		return
	}
	if os.Getenv("CC_V6_PUBLIC_ABA_STRING_CRASH") != "1" {
		t.Skip("set CC_V6_PUBLIC_ABA_STRING_CRASH=1 to run the destructive public ABA crash harness")
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestV6MapPublicABAStringCrashHarness$")
	cmd.Env = append(os.Environ(), "CC_V6_PUBLIC_ABA_STRING_CRASH_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("public ABA crash was not reproduced; output:\n%s", out)
	}
}

func runV6MapPublicABAStringCrashChild() {
	const key = "aba-key"

	readers := 16
	if s := os.Getenv("CC_V6_PUBLIC_ABA_STRING_CRASH_READERS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			os.Exit(2)
		}
		readers = n
	}
	duration := 10 * time.Second
	if s := os.Getenv("CC_V6_PUBLIC_ABA_STRING_CRASH_DURATION"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 {
			os.Exit(2)
		}
		duration = d
	}

	m := NewV6Map[string, string](WithCapacity(1))
	shortInvalid := unsafe.String((*byte)(unsafe.Pointer(uintptr(1))), 1) //nolint:all
	longValid := strings.Repeat("x", 1<<20)
	m.Store(key, shortInvalid)

	stopAt := time.Now().Add(duration)
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(readers + 1)

	go func() {
		defer wg.Done()
		for !stop.Load() {
			m.Store(key, longValid)
			m.Store(key, shortInvalid)
		}
	}()

	for range readers {
		go func() {
			defer wg.Done()
			for !stop.Load() {
				v, ok := m.Load(key)
				if !ok {
					continue
				}
				if len(v) > 1024 {
					// A valid long value survives this read. A torn value with
					// shortInvalid's data pointer and longValid's length faults.
					_ = v[0]
				}
			}
		}()
	}

	for time.Now().Before(stopAt) {
		time.Sleep(10 * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	os.Exit(0)
}

func TestV6MapStorePointerKeyValueEscapes(t *testing.T) {
	var keyMap V6Map[*int, int]
	for i := range 10 {
		key := i
		keyMap.Store(&key, i)
	}
	foundKeys := make(map[int]int)
	keyMap.Range(func(key *int, value int) bool {
		foundKeys[value] = *key
		return true
	})
	for i := range 10 {
		if got := foundKeys[i]; got != i {
			t.Fatalf("key for value %d = %d, want %d", i, got, i)
		}
	}

	var valueMap V6Map[int, *string]
	for i := range 10 {
		value := "value_" + strconv.Itoa(i)
		valueMap.Store(i, &value)
	}
	for i := range 10 {
		got, ok := valueMap.Load(i)
		want := "value_" + strconv.Itoa(i)
		if !ok || got == nil || *got != want {
			t.Fatalf("value for key %d = (%v, %v), want %q", i, got, ok, want)
		}
	}
}

func TestV6MapBasicOperations(t *testing.T) {
	m := NewV6Map[string, int](WithCapacity(8))

	if _, ok := m.Load("missing"); ok {
		t.Fatal("unexpected hit for missing key")
	}
	if actual, loaded := m.LoadOrStore("a", 1); loaded || actual != 1 {
		t.Fatalf("LoadOrStore insert = (%d, %v), want (1, false)", actual, loaded)
	}
	if actual, loaded := m.LoadOrStore("a", 2); !loaded || actual != 1 {
		t.Fatalf("LoadOrStore hit = (%d, %v), want (1, true)", actual, loaded)
	}
	if prev, ok := m.LoadAndUpdate("a", 3); !ok || prev != 1 {
		t.Fatalf("LoadAndUpdate = (%d, %v), want (1, true)", prev, ok)
	}
	if v, ok := m.Load("a"); !ok || v != 3 {
		t.Fatalf("Load = (%d, %v), want (3, true)", v, ok)
	}
	actual, loaded := m.Compute("a", func(e *MapEntry[string, int]) {
		if !e.Loaded() || e.Value() != 3 {
			t.Fatalf("Compute callback = (%d, %v), want (3, true)", e.Value(), e.Loaded())
		}
		e.Update(4)
	})
	if !loaded || actual != 4 {
		t.Fatalf("Compute update = (%d, %v), want (4, true)", actual, loaded)
	}
	if !m.CompareAndSwap("a", 4, 5) {
		t.Fatal("CompareAndSwap failed")
	}
	if m.CompareAndSwap("a", 4, 6) {
		t.Fatal("CompareAndSwap succeeded with stale value")
	}
	if !m.CompareAndDelete("a", 5) {
		t.Fatal("CompareAndDelete failed")
	}
	if _, ok := m.Load("a"); ok {
		t.Fatal("key survived CompareAndDelete")
	}
}

func TestV6MapStringSemantics(t *testing.T) {
	var m V6Map[string, int]
	stored := string([]byte("same-content"))
	lookup := string([]byte("same-content"))

	m.Store(stored, 7)
	if v, ok := m.Load(lookup); !ok || v != 7 {
		t.Fatalf("Load(equal string) = (%d, %v), want (7, true)", v, ok)
	}
}

func TestV6MapBuiltInHasherKeepsFloatSemantics(t *testing.T) {
	var m V6Map[float64, int]

	m.Store(0.0, 9)
	if v, ok := m.Load(math.Copysign(0, -1)); !ok || v != 9 {
		t.Fatalf("Load(-0) = (%d, %v), want (9, true)", v, ok)
	}
}

func TestV6MapIntHashPartsAvoidsPowerOfTwoStrideClustering(t *testing.T) {
	const buckets = uintptr(1024)
	counts := make([]int, buckets)
	for i := range buckets * v6SlotsPerBucket {
		_, start := v6HashParts(i*v6SlotsPerBucket*buckets, true, buckets-1)
		counts[start]++
	}
	nonEmpty := 0
	maxBucket := 0
	for _, c := range counts {
		if c == 0 {
			continue
		}
		nonEmpty++
		maxBucket = max(maxBucket, c)
	}
	if nonEmpty < int(buckets*3/4) || maxBucket > 8 {
		t.Fatalf("stride distribution: nonEmpty=%d maxBucket=%d, want broad distribution",
			nonEmpty, maxBucket)
	}
}

func TestV6MapHashPartsFullTagsUseHighBit(t *testing.T) {
	const mask = uintptr(1023)
	for i := range uintptr(4096) {
		intTag, _ := v6HashParts(i, true, mask)
		if intTag&0x80 == 0 {
			t.Fatalf("int tag %#x for hash %d does not use high bit", intTag, i)
		}
		hashTag, _ := v6HashParts(i*0x9e3779b9, false, mask)
		if hashTag&0x80 == 0 {
			t.Fatalf("hash tag %#x for hash %d does not use high bit", hashTag, i)
		}
	}
}

func TestV6MapSkippedWritesDoNotPublish(t *testing.T) {
	var m V6Map[int, int]
	const key = 42

	m.Store(key, key)
	table := m.table.Load()
	keyCopy := key
	_, start := v6HashParts(m.hashKey(&keyCopy), m.intKey, table.mask)
	b := table.buckets.At(start)
	ctrl := b.state.Load()

	if actual, loaded := m.LoadOrStore(key, 99); !loaded || actual != key {
		t.Fatalf("LoadOrStore = (%d, %v), want (%d, true)", actual, loaded, key)
	}
	if got := b.state.Load(); got != ctrl {
		t.Fatalf("LoadOrStore bumped ctrl: got %#x, want %#x", got, ctrl)
	}
	if previous, loaded := m.LoadAndUpdate(key, key); !loaded || previous != key {
		t.Fatalf("LoadAndUpdate = (%d, %v), want (%d, true)", previous, loaded, key)
	}
	if got := b.state.Load(); got != ctrl {
		t.Fatalf("LoadAndUpdate bumped ctrl: got %#x, want %#x", got, ctrl)
	}
	m.Store(key, key)
	if got := b.state.Load(); got != ctrl {
		t.Fatalf("Store bumped ctrl: got %#x, want %#x", got, ctrl)
	}
	if m.CompareAndSwap(key, key+1, key+2) {
		t.Fatal("CompareAndSwap with mismatched old succeeded")
	}
	if got := b.state.Load(); got != ctrl {
		t.Fatalf("failed CompareAndSwap bumped ctrl: got %#x, want %#x", got, ctrl)
	}
	if !m.CompareAndSwap(key, key, key) {
		t.Fatal("CompareAndSwap with equal old/new failed")
	}
	if value, ok := m.Load(key); !ok || value != key {
		t.Fatalf("Load after same-value CompareAndSwap = (%d, %v), want (%d, true)", value, ok, key)
	}
	ctrl = b.state.Load()
	if m.CompareAndDelete(key, key+1) {
		t.Fatal("CompareAndDelete with mismatched old succeeded")
	}
	if got := b.state.Load(); got != ctrl {
		t.Fatalf("failed CompareAndDelete bumped ctrl: got %#x, want %#x", got, ctrl)
	}
}

func TestV6MapLoadAndUpdateDoesNotInsert(t *testing.T) {
	var m V6Map[int, int]

	if previous, loaded := m.LoadAndUpdate(1, 10); loaded || previous != 0 {
		t.Fatalf("missing LoadAndUpdate = (%d, %v), want (0, false)", previous, loaded)
	}
	if _, ok := m.Load(1); ok {
		t.Fatal("LoadAndUpdate inserted missing key")
	}

	m.Store(1, 1)
	if previous, loaded := m.LoadAndDelete(1); !loaded || previous != 1 {
		t.Fatalf("LoadAndDelete = (%d, %v), want (1, true)", previous, loaded)
	}
	if previous, loaded := m.LoadAndUpdate(1, 10); loaded || previous != 0 {
		t.Fatalf("tombstone LoadAndUpdate = (%d, %v), want (0, false)", previous, loaded)
	}
	if _, ok := m.Load(1); ok {
		t.Fatal("LoadAndUpdate revived deleted key")
	}
}

func TestV6MapComputeInsertDelete(t *testing.T) {
	m := NewV6Map[string, int]()

	actual, loaded := m.Compute("k", func(e *MapEntry[string, int]) {
		if e.Loaded() {
			t.Fatal("new key reported loaded")
		}
		e.Update(10)
	})
	if loaded || actual != 10 {
		t.Fatalf("Compute insert = (%d, %v), want (10, false)", actual, loaded)
	}

	actual, loaded = m.Compute("k", func(e *MapEntry[string, int]) {
		if !e.Loaded() || e.Value() != 10 {
			t.Fatalf("loaded callback = (%d, %v), want (10, true)", e.Value(), e.Loaded())
		}
		e.Delete()
	})
	if !loaded || actual != 0 {
		t.Fatalf("Compute delete = (%d, %v), want (0, true)", actual, loaded)
	}
	if _, ok := m.Load("k"); ok {
		t.Fatal("deleted key still visible")
	}
}

func TestV6MapDeleteCompactsOnResize(t *testing.T) {
	m := NewV6Map[int, int](WithCapacity(64))
	for i := range 64 {
		m.Store(i, i)
	}
	for i := range 48 {
		m.Delete(i)
	}
	if stats := m.stats(); stats.Live != 16 || stats.Tombstones != 48 || stats.TombstoneLanes != 48 {
		t.Fatalf("stats after delete = %+v, want live=16 tombstones=48 tombstoneLanes=48", stats)
	}
	for i := 64; i < 160; i++ {
		m.Store(i, i)
	}
	for i := 48; i < 160; i++ {
		v, ok := m.Load(i)
		if !ok || v != i {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i)
		}
	}
}

func TestV6MapSameKeyTombstoneReuse(t *testing.T) {
	if !v6EnableSameKeyTombstoneReuse {
		t.Skip("same-key tombstone reuse disabled")
	}
	m := NewV6Map[int, int](
		WithCapacity(8),
		WithKeyHasher(func(int, uintptr) uintptr { return 0 }),
	)

	for i := range v6SlotsPerBucket {
		m.Store(i, i*10)
	}
	before := m.stats()
	if before.Live != v6SlotsPerBucket || before.Occupied != v6SlotsPerBucket || before.Tombstones != 0 {
		t.Fatalf("stats before delete = %+v, want live=%d occupied=%d tombstones=0", before, v6SlotsPerBucket, v6SlotsPerBucket)
	}
	if empty := v6EmptyBits(m.table.Load().buckets.At(0).state.Load()); empty != 0 {
		t.Fatalf("test bucket has empty lanes before delete: %#x", empty)
	}

	const key = 1
	if prev, ok := m.LoadAndDelete(key); !ok || prev != 10 {
		t.Fatalf("LoadAndDelete = (%d, %v), want (10, true)", prev, ok)
	}
	if empty := v6EmptyBits(m.table.Load().buckets.At(0).state.Load()); empty != 0 {
		t.Fatalf("test bucket has empty lanes after delete: %#x", empty)
	}
	if stats := m.stats(); stats.Live != before.Live-1 || stats.Occupied != before.Occupied || stats.Tombstones != 1 {
		t.Fatalf("stats after delete = %+v, want live=%d occupied=%d tombstones=1", stats, before.Live-1, before.Occupied)
	}
	if actual, loaded := m.LoadOrStore(key, 20); loaded || actual != 20 {
		t.Fatalf("LoadOrStore after delete = (%d, %v), want (20, false)", actual, loaded)
	}
	if v, ok := m.Load(key); !ok || v != 20 {
		t.Fatalf("Load revived key = (%d, %v), want (20, true)", v, ok)
	}
	if stats := m.stats(); stats.Live != before.Live || stats.Occupied != before.Occupied || stats.Tombstones != 0 {
		t.Fatalf("stats after reuse = %+v, want live=%d occupied=%d tombstones=0", stats, before.Live, before.Occupied)
	}
}

func TestV6MapDifferentKeyTombstoneReuse(t *testing.T) {
	if !v6EnableTerminalTombstoneReuse {
		t.Skip("terminal tombstone reuse disabled")
	}
	m := NewV6Map[int, int](
		WithCapacity(8),
		WithKeyHasher(func(int, uintptr) uintptr { return 0 }),
	)

	m.Store(1, 10)
	m.Store(2, 20)
	before := m.stats()
	if before.Live != 2 || before.Occupied != 2 || before.Tombstones != 0 {
		t.Fatalf("stats before delete = %+v, want live=2 occupied=2 tombstones=0", before)
	}

	m.Delete(1)
	afterDelete := m.stats()
	if afterDelete.Live != 1 || afterDelete.Occupied != before.Occupied || afterDelete.Tombstones != 1 {
		t.Fatalf("stats after delete = %+v, want live=1 occupied=%d tombstones=1", afterDelete, before.Occupied)
	}

	m.Store(3, 30)
	afterReuse := m.stats()
	if afterReuse.Live != 2 || afterReuse.Occupied != before.Occupied || afterReuse.Tombstones != 0 {
		t.Fatalf("stats after different-key reuse = %+v, want live=2 occupied=%d tombstones=0", afterReuse, before.Occupied)
	}
	if _, ok := m.Load(1); ok {
		t.Fatal("deleted key 1 was revived")
	}
	for key, want := range map[int]int{2: 20, 3: 30} {
		got, ok := m.Load(key)
		if !ok || got != want {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", key, got, ok, want)
		}
	}
}

func BenchmarkV6MapDifferentKeyTombstoneChurn(b *testing.B) {
	if !v6EnableTerminalTombstoneReuse {
		b.Skip("terminal tombstone reuse disabled")
	}
	const bucketCount = 1024
	m := NewV6Map[int, int](
		WithCapacity(bucketCount*2),
		WithKeyHasher(func(key int, _ uintptr) uintptr {
			bucket := key & (bucketCount - 1)
			tag := (key / bucketCount) & ((1 << h2Bits) - 1)
			return uintptr(bucket<<h2Bits) | uintptr(tag)
		}),
	)
	current := make([]int, bucketCount)
	for i := range bucketCount {
		current[i] = i
		m.Store(i, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket := i & (bucketCount - 1)
		old := current[bucket]
		next := old + bucketCount
		m.Delete(old)
		m.Store(next, next)
		current[bucket] = next
	}
	b.StopTimer()

	stats := m.stats()
	b.ReportMetric(float64(stats.Buckets), "buckets")
	b.ReportMetric(float64(stats.Occupied), "occupied")
	b.ReportMetric(float64(stats.Tombstones), "tombstones")
	b.ReportMetric(float64(stats.MaxProbe), "max_probe")
}

func TestV6MapStoreReusesHashAfterResizeHelp(t *testing.T) {
	var hashCalls atomic.Int32
	m := NewV6Map[int, int](
		WithCapacity(1),
		WithKeyHasher(func(key int, seed uintptr) uintptr {
			hashCalls.Add(1)
			return uintptr(key)
		}),
	)
	old := m.table.Load()
	old.nextTable.Store(newV6Table[int, int](old.bucketLen() << 1))

	m.Store(7, 70)
	if got := hashCalls.Load(); got != 1 {
		t.Fatalf("Store hash calls after resize help = %d, want 1", got)
	}

	hashCalls.Store(0)
	if v, ok := m.Load(7); !ok || v != 70 {
		t.Fatalf("Load after resize help = (%d, %v), want (70, true)", v, ok)
	}
}

func TestV6MapRangeSnapshotDoesNotRepeatBucketAfterMutation(t *testing.T) {
	m := NewV6Map[int, int](
		WithCapacity(8),
		WithKeyHasher(func(int, uintptr) uintptr { return 0 }),
	)
	for i := range 3 {
		m.Store(i, i)
	}

	seen := map[int]int{}
	mutated := false
	m.Range(func(k, v int) bool {
		seen[k]++
		if !mutated {
			mutated = true
			m.Store(2, 20)
		}
		return true
	})

	for i := range 3 {
		if seen[i] != 1 {
			t.Fatalf("Range yielded key %d %d times, want once; seen=%v", i, seen[i], seen)
		}
	}
}

func TestV6MapConcurrentInsertLoad(t *testing.T) {
	const n = 4096
	m := NewV6Map[int, int](WithCapacity(n))

	var wg sync.WaitGroup
	for g := range 8 {
		start := g * (n / 8)
		end := start + n/8
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := start; i < end; i++ {
				m.Store(i, i+1)
			}
		}()
	}
	wg.Wait()

	if got := m.Size(); got != n {
		t.Fatalf("Size = %d, want %d", got, n)
	}
	for i := range n {
		v, ok := m.Load(i)
		if !ok || v != i+1 {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i+1)
		}
	}
}

func TestV6MapConcurrentResize(t *testing.T) {
	const n = 8192
	m := NewV6Map[int, int](WithCapacity(1))

	var wg sync.WaitGroup
	for g := range 16 {
		start := g * (n / 16)
		end := start + n/16
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := start; i < end; i++ {
				m.Store(i, i+1)
			}
		}()
	}
	wg.Wait()

	if got := m.Size(); got != n {
		t.Fatalf("Size = %d, want %d", got, n)
	}
	for i := range n {
		v, ok := m.Load(i)
		if !ok || v != i+1 {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i+1)
		}
	}
}

func TestV6MapStringResize(t *testing.T) {
	const n = 3000
	m := NewV6Map[string, int]()
	for i := range n {
		m.Store(strconv.Itoa(i), i)
	}
	for i := range n {
		v, ok := m.Load(strconv.Itoa(i))
		if !ok || v != i {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i)
		}
	}
}

func TestV6MapLongProbeLimitSurvivesResizeBadHash(t *testing.T) {
	const n = v6SlotsPerBucket*64*3 + 1
	m := NewV6Map[int, int](
		WithCapacity(1),
		WithKeyHasher(func(int, uintptr) uintptr { return 0 }),
	)

	for i := range n {
		m.Store(i, i+1)
	}
	for i := range n {
		v, ok := m.Load(i)
		if !ok || v != i+1 {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i+1)
		}
	}

	table := m.table.Load()
	if table.probeLimit <= uintptr(64) {
		t.Fatalf("probeLimit = %d, want > baseline %d", table.probeLimit, 64)
	}
}

func TestV6MapLoadOrStoreFn(t *testing.T) {
	var m V6Map[string, int]

	calls := 0
	if actual, loaded := m.LoadOrStoreFn("a", func() int { calls++; return 1 }); loaded || actual != 1 {
		t.Fatalf("LoadOrStoreFn insert = (%d, %v), want (1, false)", actual, loaded)
	}
	if calls != 1 {
		t.Fatalf("valueFn calls on insert = %d, want 1", calls)
	}
	if actual, loaded := m.LoadOrStoreFn("a", func() int { calls++; return 2 }); !loaded || actual != 1 {
		t.Fatalf("LoadOrStoreFn hit = (%d, %v), want (1, true)", actual, loaded)
	}
	if calls != 1 {
		t.Fatalf("valueFn calls on hit = %d, want 1 (not invoked)", calls)
	}
}

func TestV6MapLoadOrStoreFnConcurrentUnique(t *testing.T) {
	const n = 512
	m := NewV6Map[int, int]()
	var stores atomic.Int32

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range n {
				m.LoadOrStoreFn(i, func() int {
					stores.Add(1)
					return i + 1
				})
			}
		}()
	}
	wg.Wait()

	// valueFn may be invoked more than once under contention, but exactly one
	// result wins per key and every key must be present.
	for i := range n {
		if v, ok := m.Load(i); !ok || v != i+1 {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i+1)
		}
	}
	if got := int(stores.Load()); got < n {
		t.Fatalf("valueFn invocations = %d, want >= %d", got, n)
	}
}

func TestV6MapSwap(t *testing.T) {
	m := NewV6Map[string, int]()

	if previous, loaded := m.Swap("a", 1); loaded || previous != 0 {
		t.Fatalf("Swap miss = (%d, %v), want (0, false)", previous, loaded)
	}
	if v, ok := m.Load("a"); !ok || v != 1 {
		t.Fatalf("Load after Swap insert = (%d, %v), want (1, true)", v, ok)
	}
	if previous, loaded := m.Swap("a", 2); !loaded || previous != 1 {
		t.Fatalf("Swap hit = (%d, %v), want (1, true)", previous, loaded)
	}
	if v, ok := m.Load("a"); !ok || v != 2 {
		t.Fatalf("Load after Swap update = (%d, %v), want (2, true)", v, ok)
	}

	m.Delete("a")
	if previous, loaded := m.Swap("a", 3); loaded || previous != 0 {
		t.Fatalf("Swap on tombstone = (%d, %v), want (0, false)", previous, loaded)
	}
	if v, ok := m.Load("a"); !ok || v != 3 {
		t.Fatalf("Load after tombstone Swap = (%d, %v), want (3, true)", v, ok)
	}
}

func TestV6MapSwapDedupsWithCustomEquality(t *testing.T) {
	// Swap should follow the same value dedup rule as Store/LoadAndUpdate.
	m := NewV6Map[string, int](WithValueEqual(func(int, int) bool { return true }))

	m.Store("a", 1)
	if previous, loaded := m.Swap("a", 2); !loaded || previous != 1 {
		t.Fatalf("Swap = (%d, %v), want (1, true)", previous, loaded)
	}
	if v, ok := m.Load("a"); !ok || v != 1 {
		t.Fatalf("Load after deduped Swap = (%d, %v), want (1, true)", v, ok)
	}
}

func TestV6MapCompareAndSwapStoresDespiteCustomEquality(t *testing.T) {
	// CompareAndSwap should match Map: once old matches according to EqualFunc,
	// it replaces the value with new even if EqualFunc also considers new equal.
	m := NewV6Map[string, int](WithValueEqual(func(int, int) bool { return true }))

	m.Store("a", 1)
	if !m.CompareAndSwap("a", 99, 2) {
		t.Fatal("CompareAndSwap should succeed with custom equality")
	}
	if v, ok := m.Load("a"); !ok || v != 2 {
		t.Fatalf("Load after CompareAndSwap = (%d, %v), want (2, true)", v, ok)
	}
}

func TestV6MapToMap(t *testing.T) {
	m := NewV6Map[int, int]()
	const n = 100
	for i := range n {
		m.Store(i, i*10)
	}

	full := m.ToMap()
	if len(full) != n {
		t.Fatalf("ToMap len = %d, want %d", len(full), n)
	}
	for i := range n {
		if full[i] != i*10 {
			t.Fatalf("ToMap[%d] = %d, want %d", i, full[i], i*10)
		}
	}
	// Parity with FlatMap/Map: limit <= 0 returns an empty map.
	if got := m.ToMap(-1); len(got) != 0 {
		t.Fatalf("ToMap(-1) len = %d, want 0", len(got))
	}
	if got := m.ToMap(0); len(got) != 0 {
		t.Fatalf("ToMap(0) len = %d, want 0", len(got))
	}
	if got := m.ToMap(7); len(got) != 7 {
		t.Fatalf("ToMap(7) len = %d, want 7", len(got))
	}

	var empty V6Map[int, int]
	if got := empty.ToMap(); len(got) != 0 {
		t.Fatalf("zero-value ToMap len = %d, want 0", len(got))
	}
}

func TestV6MapComputeRange(t *testing.T) {
	m := NewV6Map[int, int]()
	const n = 64
	for i := range n {
		m.Store(i, i)
	}

	// Read-only pass observes every entry exactly once.
	seen := map[int]int{}
	m.ComputeRange(func(e *MapEntry[int, int]) bool {
		if !e.Loaded() {
			t.Fatal("ComputeRange yielded unloaded entry")
		}
		seen[e.Key()]++
		return true
	})
	if len(seen) != n {
		t.Fatalf("ComputeRange visited %d keys, want %d", len(seen), n)
	}
	for k, c := range seen {
		if c != 1 {
			t.Fatalf("key %d visited %d times, want 1", k, c)
		}
	}

	// Update all even keys, delete all odd keys.
	m.ComputeRange(func(e *MapEntry[int, int]) bool {
		if e.Key()%2 == 0 {
			e.Update(e.Value() + 1000)
		} else {
			e.Delete()
		}
		return true
	})
	if got := m.Size(); got != n/2 {
		t.Fatalf("Size after ComputeRange = %d, want %d", got, n/2)
	}
	for i := range n {
		v, ok := m.Load(i)
		if i%2 == 0 {
			if !ok || v != i+1000 {
				t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i+1000)
			}
		} else if ok {
			t.Fatalf("deleted key %d still present with %d", i, v)
		}
	}

	// Early stop.
	visited := 0
	m.ComputeRange(func(e *MapEntry[int, int]) bool {
		visited++
		return false
	})
	if visited != 1 {
		t.Fatalf("early-stop visited %d entries, want 1", visited)
	}

	var empty V6Map[int, int]
	empty.ComputeRange(func(e *MapEntry[int, int]) bool {
		t.Fatal("zero-value ComputeRange yielded an entry")
		return false
	})
}

func TestV6MapEntries(t *testing.T) {
	m := NewV6Map[int, int]()
	for i := range 16 {
		m.Store(i, i)
	}
	count := 0
	for e := range m.Entries() {
		if !e.Loaded() {
			t.Fatal("Entries yielded unloaded entry")
		}
		if e.Key() == 3 {
			e.Delete()
		}
		count++
	}
	if count != 16 {
		t.Fatalf("Entries visited %d, want 16", count)
	}
	if _, ok := m.Load(3); ok {
		t.Fatal("Entries Delete did not remove key")
	}
	if got := m.Size(); got != 15 {
		t.Fatalf("Size = %d, want 15", got)
	}
}

func TestV6MapComputeRangeConcurrentWriters(t *testing.T) {
	const n = 4096
	m := NewV6Map[int, int](WithCapacity(n))
	for i := range n {
		m.Store(i, 0)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := g
			for {
				select {
				case <-stop:
					return
				default:
				}
				m.Store(n+i, i)
				m.Delete(n + i)
				i += 4
				if i > 4*n {
					i = g
				}
			}
		}()
	}

	for range 8 {
		visited := 0
		m.ComputeRange(func(e *MapEntry[int, int]) bool {
			if e.Key() < n {
				e.Update(e.Value() + 1)
			}
			visited++
			return true
		})
		if visited < n/2 {
			t.Fatalf("ComputeRange visited only %d entries", visited)
		}
	}
	close(stop)
	wg.Wait()

	// Every completed pass visits each persistent key at least once; resize
	// restarts may re-visit (weakly consistent), so 8 is a lower bound.
	for i := range n {
		v, ok := m.Load(i)
		if !ok || v < 8 {
			t.Fatalf("Load(%d) = (%d, %v), want >= 8", i, v, ok)
		}
	}
}

func TestV6MapGrowAvoidsResize(t *testing.T) {
	const n = 10_000
	m := NewV6Map[int, int]()
	m.Grow(n)
	table := m.table.Load()
	if table == nil {
		t.Fatal("table is nil after Grow")
	}
	for i := range n {
		m.Store(i, i)
	}
	if got := m.table.Load(); got != table {
		t.Fatalf("table replaced during inserts after Grow(%d)", n)
	}
	if got := m.Size(); got != n {
		t.Fatalf("Size = %d, want %d", got, n)
	}

	// Growing within existing capacity is a no-op.
	before := m.table.Load()
	m.Grow(1)
	if got := m.table.Load(); got != before {
		t.Fatal("Grow within capacity replaced the table")
	}
	m.Grow(-5)
	m.Grow(0)
}

func TestV6MapShrink(t *testing.T) {
	const n = 10_000
	m := NewV6Map[int, int]()
	for i := range n {
		m.Store(i, i)
	}
	grown := m.table.Load().bucketLen()
	for i := range n {
		if i >= 64 {
			m.Delete(i)
		}
	}
	m.Shrink()
	table := m.table.Load()
	if table.bucketLen() >= grown {
		t.Fatalf("Shrink kept bucketLen %d, want < %d", table.bucketLen(), grown)
	}
	if got := m.Size(); got != 64 {
		t.Fatalf("Size after Shrink = %d, want 64", got)
	}
	for i := range 64 {
		if v, ok := m.Load(i); !ok || v != i {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i)
		}
	}
	if stats := m.stats(); stats.Tombstones != 0 {
		t.Fatalf("Shrink left %d tombstones, want 0 (compacted)", stats.Tombstones)
	}

	// Shrink never goes below the configured capacity.
	c := NewV6Map[int, int](WithCapacity(4096))
	c.Store(1, 1)
	before := c.table.Load().bucketLen()
	c.Shrink()
	if got := c.table.Load().bucketLen(); got != before {
		t.Fatalf("Shrink went below WithCapacity: %d -> %d", before, got)
	}

	var empty V6Map[int, int]
	empty.Shrink()
}

func TestV6MapGrowShrinkConcurrentDataIntegrity(t *testing.T) {
	const n = 4096
	m := NewV6Map[int, int]()
	for i := range n {
		m.Store(i, i)
	}

	var resizers sync.WaitGroup
	stop := make(chan struct{})
	for range 2 {
		resizers.Add(1)
		go func() {
			defer resizers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				m.Grow(n * 4)
				m.Shrink()
			}
		}()
	}
	var writers sync.WaitGroup
	for g := range 4 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := range 1000 {
				k := n + g*1000 + i
				m.Store(k, k)
				if v, ok := m.Load(k); !ok || v != k {
					t.Errorf("Load(%d) = (%d, %v) during resize churn", k, v, ok)
					return
				}
				m.Delete(k)
			}
		}()
	}
	writers.Wait()
	close(stop)
	resizers.Wait()

	if got := m.Size(); got != n {
		t.Fatalf("Size = %d, want %d", got, n)
	}
	for i := range n {
		if v, ok := m.Load(i); !ok || v != i {
			t.Fatalf("Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i)
		}
	}
}

func TestV6MapCloneTo(t *testing.T) {
	src := NewV6Map[string, int](
		WithKeyHasher(func(key string, seed uintptr) uintptr {
			return uintptr(len(key)) * 31
		}),
	)
	const n = 500
	for i := range n {
		src.Store(strconv.Itoa(i), i)
	}

	clone := NewV6Map[string, int]()
	clone.Store("stale", 999)
	src.CloneTo(clone)

	if _, ok := clone.Load("stale"); ok {
		t.Fatal("CloneTo did not clear destination")
	}
	if got := clone.Size(); got != n {
		t.Fatalf("clone Size = %d, want %d", got, n)
	}
	for i := range n {
		if v, ok := clone.Load(strconv.Itoa(i)); !ok || v != i {
			t.Fatalf("clone Load(%d) = (%d, %v), want (%d, true)", i, v, ok, i)
		}
	}
	if clone.seed != src.seed || clone.minLen != src.minLen || clone.intKey != src.intKey {
		t.Fatal("CloneTo did not copy configuration")
	}

	// Cloning into a zero-value destination.
	var fresh V6Map[string, int]
	src.CloneTo(&fresh)
	if got := fresh.Size(); got != n {
		t.Fatalf("zero-value clone Size = %d, want %d", got, n)
	}

	// Cloning an empty source empties the destination.
	empty := NewV6Map[string, int]()
	empty.CloneTo(clone)
	if got := clone.Size(); got != 0 {
		t.Fatalf("clone of empty source Size = %d, want 0", got)
	}
}

func TestV6MapRebuildOperations(t *testing.T) {
	m := NewV6Map[int, int]()
	for i := range 32 {
		m.Store(i, i)
	}

	m.Rebuild(func(r *MapRebuild[int, int]) {
		if v, ok := r.Load(5); !ok || v != 5 {
			t.Fatalf("rebuild Load(5) = (%d, %v), want (5, true)", v, ok)
		}
		r.Store(100, 100)
		if previous, loaded := r.Swap(100, 101); !loaded || previous != 100 {
			t.Fatalf("rebuild Swap = (%d, %v), want (100, true)", previous, loaded)
		}
		if actual, loaded := r.LoadOrStore(101, 1); loaded || actual != 1 {
			t.Fatalf("rebuild LoadOrStore = (%d, %v), want (1, false)", actual, loaded)
		}
		if previous, loaded := r.LoadAndDelete(0); !loaded || previous != 0 {
			t.Fatalf("rebuild LoadAndDelete = (%d, %v), want (0, true)", previous, loaded)
		}
		r.Delete(1)
		r.ComputeRange(func(e *MapEntry[int, int]) bool {
			if e.Key() == 2 {
				e.Update(2000)
			}
			return true
		})
		count := 0
		for range r.Entries() {
			count++
		}
		if got := r.Size(); got != count || got != 32 {
			t.Fatalf("rebuild Size = %d, Entries count = %d, want 32", got, count)
		}
		if got := len(r.ToMap()); got != 32 {
			t.Fatalf("rebuild ToMap len = %d, want 32", got)
		}
	})

	if _, ok := m.Load(0); ok {
		t.Fatal("rebuild LoadAndDelete did not remove key 0")
	}
	if _, ok := m.Load(1); ok {
		t.Fatal("rebuild Delete did not remove key 1")
	}
	if v, ok := m.Load(2); !ok || v != 2000 {
		t.Fatalf("rebuild ComputeRange update: Load(2) = (%d, %v), want (2000, true)", v, ok)
	}
	if v, ok := m.Load(100); !ok || v != 101 {
		t.Fatalf("rebuild Store/Swap: Load(100) = (%d, %v), want (101, true)", v, ok)
	}
}

func TestV6MapRebuildBlocksWritersAllowsReaders(t *testing.T) {
	m := NewV6Map[int, int]()
	m.Store(1, 1)

	inRebuild := make(chan struct{})
	release := make(chan struct{})
	rebuildDone := make(chan struct{})
	go func() {
		m.Rebuild(func(r *MapRebuild[int, int]) {
			close(inRebuild)
			<-release
		})
		close(rebuildDone)
	}()
	<-inRebuild

	writes := make(chan struct{}, 4)
	writeOps := []func(){
		func() { m.Store(2, 2) },
		func() { m.Delete(1) },
		func() { m.Compute(3, func(e *MapEntry[int, int]) { e.Update(3) }) },
		func() { m.Swap(4, 4) },
	}
	for _, op := range writeOps {
		go func() {
			op()
			writes <- struct{}{}
		}()
	}
	select {
	case <-writes:
		t.Fatal("a writer completed during rebuild")
	case <-time.After(100 * time.Millisecond):
	}

	// Readers proceed during rebuild.
	if v, ok := m.Load(1); !ok || v != 1 {
		t.Fatalf("Load during rebuild = (%d, %v), want (1, true)", v, ok)
	}
	if got := m.Size(); got != 1 {
		t.Fatalf("Size during rebuild = %d, want 1", got)
	}

	close(release)
	<-rebuildDone
	for range writeOps {
		select {
		case <-writes:
		case <-time.After(2 * time.Second):
			t.Fatal("writer did not complete after rebuild ended")
		}
	}
	if v, ok := m.Load(2); !ok || v != 2 {
		t.Fatalf("Load(2) after rebuild = (%d, %v), want (2, true)", v, ok)
	}
	if _, ok := m.Load(1); ok {
		t.Fatal("Delete(1) blocked during rebuild was lost afterwards")
	}
}

func TestV6MapClearDuringConcurrentWrites(t *testing.T) {
	m := NewV6Map[int, int]()
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				m.Store(g*2000+i, i)
			}
		}()
	}
	for range 8 {
		m.Clear()
	}
	wg.Wait()
	m.Clear()
	if got := m.Size(); got != 0 {
		t.Fatalf("Size after final Clear = %d, want 0", got)
	}
	count := 0
	m.Range(func(int, int) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("Range after final Clear yielded %d entries, want 0", count)
	}
}

func TestV6MapBucketAndEntryLayout(t *testing.T) {
	size := unsafe.Sizeof(v6Bucket{})
	if size != 8 {
		t.Fatalf("bucket size = %d, want 8", size)
	}
	table := newV6Table[int, int](v6MinBuckets)
	bucketBase := uintptr(unsafe.Pointer(table.buckets.At(0)))
	if uintptr(unsafe.Pointer(table.buckets.At(1)))-bucketBase != size {
		t.Fatal("bucket stride mismatch")
	}
	entryBase := uintptr(unsafe.Pointer(table.entries.At(0)))
	align := unsafe.Alignof(v6Entry[int, int]{})
	if entryBase&(uintptr(align)-1) != 0 {
		t.Fatalf("entry base = %#x, want %d-byte aligned", entryBase, align)
	}
}
