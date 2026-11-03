//go:build !race && goexperiment.simd && go1.28

package cc

import (
	"strconv"
	"sync"
	"testing"
	"unsafe"
)

const (
	benchmarkV28StoreMissBatch     = 1 << 16
	benchmarkV28StoreMissKeyMaxLen = 20
)

var (
	benchmarkV28StringStoreMissKeysOnce sync.Once
	benchmarkV28StringStoreMissKeys     []string
	benchmarkV28StringStoreMissKeyArena []byte
)

func BenchmarkV28MapStringLoadHit(b *testing.B) {
	benchmarkV28StringLoadHit(b, "V28", func(capacity int) interface {
		Load(string) (int, bool)
		Store(string, int)
	} {
		return NewV28Map[string, int](WithCapacity(capacity))
	})
	benchmarkV28StringLoadHit(b, "OFHT", func(capacity int) interface {
		Load(string) (int, bool)
		Store(string, int)
	} {
		return NewOFHTMap[string, int](WithCapacity(capacity))
	})
}

func BenchmarkV28MapIntLoadHit(b *testing.B) {
	benchmarkV28IntLoadHit(b, "V28", func(capacity int) interface {
		Load(int) (int, bool)
		Store(int, int)
	} {
		return NewV28Map[int, int](WithCapacity(capacity))
	})
	benchmarkV28IntLoadHit(b, "OFHT", func(capacity int) interface {
		Load(int) (int, bool)
		Store(int, int)
	} {
		return NewOFHTMap[int, int](WithCapacity(capacity))
	})
}

func BenchmarkV28MapStringLoadMiss(b *testing.B) {
	benchmarkV28StringLoadMiss(b, "V28", func(capacity int) interface {
		Load(string) (int, bool)
		Store(string, int)
	} {
		return NewV28Map[string, int](WithCapacity(capacity))
	})
	benchmarkV28StringLoadMiss(b, "OFHT", func(capacity int) interface {
		Load(string) (int, bool)
		Store(string, int)
	} {
		return NewOFHTMap[string, int](WithCapacity(capacity))
	})
}

func BenchmarkV28MapIntLoadMiss(b *testing.B) {
	benchmarkV28IntLoadMiss(b, "V28", func(capacity int) interface {
		Load(int) (int, bool)
		Store(int, int)
	} {
		return NewV28Map[int, int](WithCapacity(capacity))
	})
	benchmarkV28IntLoadMiss(b, "OFHT", func(capacity int) interface {
		Load(int) (int, bool)
		Store(int, int)
	} {
		return NewOFHTMap[int, int](WithCapacity(capacity))
	})
}

func BenchmarkV28MapStringStoreHit(b *testing.B) {
	benchmarkV28StringStoreHit(b, "V28", func(capacity int) interface {
		Store(string, int)
	} {
		return NewV28Map[string, int](WithCapacity(capacity))
	})
	benchmarkV28StringStoreHit(b, "OFHT", func(capacity int) interface {
		Store(string, int)
	} {
		return NewOFHTMap[string, int](WithCapacity(capacity))
	})
}

func BenchmarkV28MapIntStoreHit(b *testing.B) {
	benchmarkV28IntStoreHit(b, "V28", func(capacity int) interface {
		Store(int, int)
	} {
		return NewV28Map[int, int](WithCapacity(capacity))
	})
	benchmarkV28IntStoreHit(b, "OFHT", func(capacity int) interface {
		Store(int, int)
	} {
		return NewOFHTMap[int, int](WithCapacity(capacity))
	})
}

func BenchmarkV28MapStringStoreMiss(b *testing.B) {
	benchmarkV28StringStoreMiss(b, "V28", func(capacity int) interface {
		Store(string, int)
	} {
		return NewV28Map[string, int](WithCapacity(capacity))
	})
	benchmarkV28StringStoreMiss(b, "OFHT", func(capacity int) interface {
		Store(string, int)
	} {
		return NewOFHTMap[string, int](WithCapacity(capacity))
	})
}

func BenchmarkV28MapIntStoreMiss(b *testing.B) {
	benchmarkV28IntStoreMiss(b, "V28", func(capacity int) interface {
		Store(int, int)
	} {
		return NewV28Map[int, int](WithCapacity(capacity))
	})
	benchmarkV28IntStoreMiss(b, "OFHT", func(capacity int) interface {
		Store(int, int)
	} {
		return NewOFHTMap[int, int](WithCapacity(capacity))
	})
}

func benchmarkV28StoreMissStringKeys() []string {
	benchmarkV28StringStoreMissKeysOnce.Do(func() {
		keys := make([]string, benchmarkV28StoreMissBatch)
		arena := make([]byte, benchmarkV28StoreMissBatch*benchmarkV28StoreMissKeyMaxLen)
		for i := range keys {
			offset := i * benchmarkV28StoreMissKeyMaxLen
			n := benchmarkV28WriteDecimal(arena[offset:offset+benchmarkV28StoreMissKeyMaxLen], uint(i))
			keys[i] = unsafe.String(&arena[offset], n)
		}
		benchmarkV28StringStoreMissKeyArena = arena
		benchmarkV28StringStoreMissKeys = keys
	})
	return benchmarkV28StringStoreMissKeys
}

func benchmarkV28WriteDecimal(dst []byte, value uint) int {
	if value == 0 {
		dst[0] = '0'
		return 1
	}
	var buf [benchmarkV28StoreMissKeyMaxLen]byte
	n := 0
	for value > 0 {
		n++
		buf[len(buf)-n] = byte('0' + value%10)
		value /= 10
	}
	copy(dst, buf[len(buf)-n:])
	return n
}

func benchmarkV28StringLoadHit(
	b *testing.B,
	name string,
	newMap func(capacity int) interface {
		Load(string) (int, bool)
		Store(string, int)
	},
) {
	const n = 1 << 16
	keys := make([]string, n)
	m := newMap(n)
	for i := 0; i < n; i++ {
		keys[i] = strconv.Itoa(i)
		m.Store(keys[i], i)
	}

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		mask := n - 1
		for i := 0; i < b.N; i++ {
			v, ok := m.Load(keys[i&mask])
			if !ok || v != i&mask {
				b.Fatalf("Load mismatch: got (%d, %v)", v, ok)
			}
		}
	})
}

func benchmarkV28IntLoadHit(
	b *testing.B,
	name string,
	newMap func(capacity int) interface {
		Load(int) (int, bool)
		Store(int, int)
	},
) {
	const n = 1 << 16
	m := newMap(n)
	for i := 0; i < n; i++ {
		m.Store(i, i)
	}

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		mask := n - 1
		for i := 0; i < b.N; i++ {
			key := i & mask
			v, ok := m.Load(key)
			if !ok || v != key {
				b.Fatalf("Load mismatch: got (%d, %v)", v, ok)
			}
		}
	})
}

func benchmarkV28StringLoadMiss(
	b *testing.B,
	name string,
	newMap func(capacity int) interface {
		Load(string) (int, bool)
		Store(string, int)
	},
) {
	const n = 1 << 16
	keys := make([]string, n)
	m := newMap(n)
	for i := 0; i < n; i++ {
		keys[i] = strconv.Itoa(i + n)
		m.Store(strconv.Itoa(i), i)
	}

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		mask := n - 1
		for i := 0; i < b.N; i++ {
			if v, ok := m.Load(keys[i&mask]); ok {
				b.Fatalf("unexpected hit: (%d, %v)", v, ok)
			}
		}
	})
}

func benchmarkV28IntLoadMiss(
	b *testing.B,
	name string,
	newMap func(capacity int) interface {
		Load(int) (int, bool)
		Store(int, int)
	},
) {
	const n = 1 << 16
	m := newMap(n)
	for i := 0; i < n; i++ {
		m.Store(i, i)
	}

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		mask := n - 1
		for i := 0; i < b.N; i++ {
			if v, ok := m.Load((i & mask) + n); ok {
				b.Fatalf("unexpected hit: (%d, %v)", v, ok)
			}
		}
	})
}

func benchmarkV28StringStoreHit(
	b *testing.B,
	name string,
	newMap func(capacity int) interface {
		Store(string, int)
	},
) {
	const n = 1 << 16
	keys := make([]string, n)
	m := newMap(n)
	for i := 0; i < n; i++ {
		keys[i] = strconv.Itoa(i)
		m.Store(keys[i], i)
	}

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		mask := n - 1
		for i := 0; i < b.N; i++ {
			m.Store(keys[i&mask], i+n)
		}
	})
}

func benchmarkV28IntStoreHit(
	b *testing.B,
	name string,
	newMap func(capacity int) interface {
		Store(int, int)
	},
) {
	const n = 1 << 16
	m := newMap(n)
	for i := 0; i < n; i++ {
		m.Store(i, i)
	}

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		mask := n - 1
		for i := 0; i < b.N; i++ {
			m.Store(i&mask, i+n)
		}
	})
}

func benchmarkV28StringStoreMiss(
	b *testing.B,
	name string,
	newMap func(capacity int) interface {
		Store(string, int)
	},
) {
	keys := benchmarkV28StoreMissStringKeys()

	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.StopTimer()
		for done := 0; done < b.N; {
			m := newMap(len(keys))
			limit := min(len(keys), b.N-done)

			b.StartTimer()
			for i := 0; i < limit; i++ {
				m.Store(keys[i], done+i)
			}
			b.StopTimer()
			done += limit
		}
	})
}

func benchmarkV28IntStoreMiss(
	b *testing.B,
	name string,
	newMap func(capacity int) interface {
		Store(int, int)
	},
) {
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.StopTimer()
		for done := 0; done < b.N; {
			m := newMap(benchmarkV28StoreMissBatch)
			limit := min(benchmarkV28StoreMissBatch, b.N-done)

			b.StartTimer()
			for i := 0; i < limit; i++ {
				m.Store(i, done+i)
			}
			b.StopTimer()
			done += limit
		}
	})
}
