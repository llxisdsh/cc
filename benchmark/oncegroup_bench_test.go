package benchmark

import (
	"testing"

	"github.com/llxisdsh/cc"
	xsf "golang.org/x/sync/singleflight"
)

// -------------------------
// Benchmarks
// -------------------------

// Same-key contention benchmark.
func BenchmarkOnceGroupSameKey(b *testing.B) {
	b.ReportAllocs()
	var g cc.OnceGroup[string, any]
	key := "same"

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = g.Do(key, func() (any, error) { return 1, nil })
		}
	})
}

// Same-key contention benchmark (x/sync).
func BenchmarkOnceGroupSameKey_SingleFlight(b *testing.B) {
	b.ReportAllocs()
	var g xsf.Group
	key := "same"

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = g.Do(key, func() (any, error) { return 1, nil })
		}
	})
}

// Many keys benchmark.
func BenchmarkOnceGroupManyKeys(b *testing.B) {
	b.ReportAllocs()
	var g cc.OnceGroup[string, any]

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k_" + strconvSmall(i&1023)
			_, _, _ = g.Do(key, func() (any, error) { return i, nil })
			i++
		}
	})
}

// Many keys benchmark (x/sync).
func BenchmarkOnceGroupManyKeys_SingleFlight(b *testing.B) {
	b.ReportAllocs()
	var g xsf.Group

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k_" + strconvSmall(i&1023)
			_, _, _ = g.Do(key, func() (any, error) { return i, nil })
			i++
		}
	})
}

// DoChan same-key contention benchmark.
func BenchmarkOnceGroupDoChanSameKey(b *testing.B) {
	b.ReportAllocs()
	var g cc.OnceGroup[string, any]
	key := "same"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := g.DoChan(key, func() (any, error) {
				heavyWork(128)
				return 1, nil
			})
			_ = <-ch
		}
	})
}

// DoChan same-key contention benchmark (x/sync).
func BenchmarkOnceGroupDoChanSameKey_SingleFlight(b *testing.B) {
	b.ReportAllocs()
	var g xsf.Group
	key := "same"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := g.DoChan(key, func() (any, error) {
				heavyWork(128)
				return 1, nil
			})
			_ = <-ch
		}
	})
}

// DoChan many-keys benchmark.
func BenchmarkOnceGroupDoChanManyKeys(b *testing.B) {
	b.ReportAllocs()
	var g cc.OnceGroup[string, any]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k_" + strconvSmall(i&1023)
			ch := g.DoChan(key, func() (any, error) {
				heavyWork(64)
				return i, nil
			})
			_ = <-ch
			i++
		}
	})
}

// DoChan many-keys benchmark (x/sync).
func BenchmarkOnceGroupDoChanManyKeys_SingleFlight(b *testing.B) {
	b.ReportAllocs()
	var g xsf.Group
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k_" + strconvSmall(i&1023)
			ch := g.DoChan(key, func() (any, error) {
				heavyWork(64)
				return i, nil
			})
			_ = <-ch
			i++
		}
	})
}

// Heavy fn workload benchmark under same key.
func BenchmarkOnceGroupHeavyWorkSameKey(b *testing.B) {
	b.ReportAllocs()
	var g cc.OnceGroup[string, any]
	key := "heavy"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = g.Do(key, func() (any, error) {
				heavyWork(128)
				return 1, nil
			})
		}
	})
}

// Heavy fn workload benchmark under same key (x/sync).
func BenchmarkOnceGroupHeavyWorkSameKey_SingleFlight(b *testing.B) {
	b.ReportAllocs()
	var g xsf.Group
	key := "heavy"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = g.Do(key, func() (any, error) {
				heavyWork(128)
				return 1, nil
			})
		}
	})
}

// Heavy fn workload benchmark under many keys.
func BenchmarkOnceGroupHeavyWorkManyKeys(b *testing.B) {
	b.ReportAllocs()
	var g cc.OnceGroup[string, any]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k_" + strconvSmall(i&1023)
			_, _, _ = g.Do(key, func() (any, error) {
				heavyWork(64)
				return i, nil
			})
			i++
		}
	})
}

// Heavy fn workload benchmark under many keys (x/sync).
func BenchmarkOnceGroupHeavyWorkManyKeys_SingleFlight(b *testing.B) {
	b.ReportAllocs()
	var g xsf.Group
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "k_" + strconvSmall(i&1023)
			_, _, _ = g.Do(key, func() (any, error) {
				heavyWork(64)
				return i, nil
			})
			i++
		}
	})
}

// Simple CPU-heavy loop to simulate workload.
func heavyWork(n int) int {
	x := 0
	for i := 0; i < n; i++ {
		x ^= i * 31
		x += i >> 1
	}
	return x
}

// strconvSmall is a tiny integer-to-string helper to avoid imports.
func strconvSmall(i int) string {
	// Fast path for small non-negative ints.
	if i < 10 {
		return string('0' + byte(i))
	}
	// Fallback: minimal heap-free conversion for 0..1023.
	var buf [4]byte
	n := 0
	for i >= 10 {
		d := i % 10
		buf[3-n] = '0' + byte(d)
		i /= 10
		n++
	}
	buf[3-n] = '0' + byte(i)
	return string(buf[3-n : 4])
}
