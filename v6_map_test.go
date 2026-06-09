//go:build !race

package cc

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
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
	for i := uintptr(0); i < buckets*v6SlotsPerBucket; i++ {
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
	for i := uintptr(0); i < 4096; i++ {
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

func TestV6MapNoOpWritesDoNotPublish(t *testing.T) {
	var m V6Map[int, int]
	const key = 42

	m.Store(key, key)
	table := m.table.Load()
	if table == nil {
		t.Fatal("table is nil")
	}
	keyCopy := key
	_, start := v6HashParts(m.hashKey(&keyCopy), table.intKey, table.mask)
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
	if got := b.state.Load(); got != ctrl {
		t.Fatalf("same-value CompareAndSwap bumped ctrl: got %#x, want %#x", got, ctrl)
	}
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
	if stats := m.stats(); stats.Live != 16 || stats.Deleted != 48 || stats.TombstoneLanes != 48 {
		t.Fatalf("stats after delete = %+v, want live=16 deleted=48 tombstones=48", stats)
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

	m.Store(1, 10)
	before := m.stats()
	if prev, ok := m.LoadAndDelete(1); !ok || prev != 10 {
		t.Fatalf("LoadAndDelete = (%d, %v), want (10, true)", prev, ok)
	}
	if stats := m.stats(); stats.Live != 0 || stats.Used != before.Used || stats.Deleted != 1 {
		t.Fatalf("stats after delete = %+v, want live=0 used=%d deleted=1", stats, before.Used)
	}
	if actual, loaded := m.LoadOrStore(1, 20); loaded || actual != 20 {
		t.Fatalf("LoadOrStore after delete = (%d, %v), want (20, false)", actual, loaded)
	}
	if v, ok := m.Load(1); !ok || v != 20 {
		t.Fatalf("Load revived key = (%d, %v), want (20, true)", v, ok)
	}
	if stats := m.stats(); stats.Live != 1 || stats.Used != before.Used || stats.Deleted != 0 {
		t.Fatalf("stats after reuse = %+v, want live=1 used=%d deleted=0", stats, before.Used)
	}
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
	if old == nil {
		t.Fatal("table is nil")
	}
	old.nextTable.Store(newV6Table[int, int](old.bucketLen()<<1, old.intKey))

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
	const n = v6SlotsPerBucket*v6MaxProbeBuckets*3 + 1
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
	if table == nil {
		t.Fatal("table is nil")
	}
	if table.probeLimit <= uintptr(v6MaxProbeBuckets) {
		t.Fatalf("probeLimit = %d, want > baseline %d", table.probeLimit, v6MaxProbeBuckets)
	}
}

func TestV6MapBucketAndEntryLayout(t *testing.T) {
	size := unsafe.Sizeof(v6Bucket{})
	if size != 8 {
		t.Fatalf("bucket size = %d, want 8", size)
	}
	table := newV6Table[int, int](v6MinBuckets, true)
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
