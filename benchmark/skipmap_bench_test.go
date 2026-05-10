package benchmark

import (
	"strconv"
	"testing"

	"github.com/llxisdsh/cc"
	"github.com/zhangyunhao116/skipmap"
)

const initSize = 1 << 10 // for mixed Benchmark

func Benchmark_SkipMapStore(b *testing.B) {
	b.Run("cc.SkipMap", func(b *testing.B) {
		l := cc.NewSkipMap[int64, any]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				l.Store(int64(runtime_cheaprand()), nil)
			}
		})
	})
	b.Run("zhangyunhao116.SkipMap", func(b *testing.B) {
		l := skipmap.New[int64, any]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				l.Store(int64(runtime_cheaprand()), nil)
			}
		})
	})
	// b.Run("sync.Map", func(b *testing.B) {
	// 	var l sync.Map
	// 	b.ResetTimer()
	// 	b.RunParallel(func(pb *testing.PB) {
	// 		for pb.Next() {
	// 			l.Store(int64(runtime_cheaprand()), nil)
	// 		}
	// 	})
	// })
}

func Benchmark_SkipMapLoad100Hits(b *testing.B) {
	b.Run("cc.SkipMap", func(b *testing.B) {
		l := cc.NewSkipMap[int64, any]()
		for i := range initSize {
			l.Store(int64(i), nil)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = l.Load(int64(runtime_cheaprand() % initSize))
			}
		})
	})

	b.Run("zhangyunhao116.SkipMap", func(b *testing.B) {
		l := skipmap.New[int64, any]()
		for i := range initSize {
			l.Store(int64(i), nil)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = l.Load(int64(runtime_cheaprand() % initSize))
			}
		})
	})
	// b.Run("sync.Map", func(b *testing.B) {
	// 	var l sync.Map
	// 	for i := 0; i < initsize; i++ {
	// 		l.Store(int64(i), nil)
	// 	}
	// 	b.ResetTimer()
	// 	b.RunParallel(func(pb *testing.PB) {
	// 		for pb.Next() {
	// 			_, _ = l.Load(int64(runtime_cheaprand() % initSize))
	// 		}
	// 	})
	// })
}

func Benchmark_SkipMap50Store50Load(b *testing.B) {
	b.Run("cc.SkipMap", func(b *testing.B) {
		l := cc.NewSkipMap[int64, any]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				u := runtime_cheaprand() % 10
				if u < 5 {
					l.Store(int64(runtime_cheaprand()), nil)
				} else {
					l.Load(int64(runtime_cheaprand()))
				}
			}
		})
	})

	b.Run("zhangyunhao116.SkipMap", func(b *testing.B) {
		l := skipmap.New[int64, any]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				u := runtime_cheaprand() % 10
				if u < 5 {
					l.Store(int64(runtime_cheaprand()), nil)
				} else {
					l.Load(int64(runtime_cheaprand()))
				}
			}
		})
	})
	// b.Run("sync.Map", func(b *testing.B) {
	// 	var l sync.Map
	// 	b.ResetTimer()
	// 	b.RunParallel(func(pb *testing.PB) {
	// 		for pb.Next() {
	// 			u := runtime_cheaprand() % 10
	// 			if u < 5 {
	// 				l.Store(int64(runtime_cheaprand()), nil)
	// 			} else {
	// 				l.Load(int64(runtime_cheaprand()))
	// 			}
	// 		}
	// 	})
	// })
}

func Benchmark_SkipMap30Store70Load(b *testing.B) {
	b.Run("cc.SkipMap", func(b *testing.B) {
		l := cc.NewSkipMap[int64, any]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				u := runtime_cheaprand() % 10
				if u < 3 {
					l.Store(int64(runtime_cheaprand()), nil)
				} else {
					l.Load(int64(runtime_cheaprand()))
				}
			}
		})
	})

	b.Run("zhangyunhao116.SkipMap", func(b *testing.B) {
		l := skipmap.New[int64, any]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				u := runtime_cheaprand() % 10
				if u < 3 {
					l.Store(int64(runtime_cheaprand()), nil)
				} else {
					l.Load(int64(runtime_cheaprand()))
				}
			}
		})
	})
}

func Benchmark_SkipMapStringStore(b *testing.B) {
	b.Run("cc.SkipMap", func(b *testing.B) {
		l := cc.NewSkipMap[string, any]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				l.Store(strconv.Itoa(int(runtime_cheaprand())), nil)
			}
		})
	})

	b.Run("zhangyunhao116.SkipMap", func(b *testing.B) {
		l := skipmap.New[string, any]()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				l.Store(strconv.Itoa(int(runtime_cheaprand())), nil)
			}
		})
	})
}

func Benchmark_SkipMapString1Delete9Store90Load(b *testing.B) {
	b.Run("cc.SkipMap", func(b *testing.B) {
		l := cc.NewSkipMap[string, any]()
		for i := 0; i < initSize; i++ {
			l.Store(strconv.Itoa(i), nil)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				u := runtime_cheaprand() % 100
				if u < 9 {
					l.Store(strconv.Itoa(int(runtime_cheaprand())), nil)
				} else if u == 10 {
					l.Delete(strconv.Itoa(int(runtime_cheaprand())))
				} else {
					l.Load(strconv.Itoa(int(runtime_cheaprand())))
				}
			}
		})
	})

	b.Run("zhangyunhao116.SkipMap", func(b *testing.B) {
		l := skipmap.New[string, any]()
		for i := 0; i < initSize; i++ {
			l.Store(strconv.Itoa(i), nil)
		}
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				u := runtime_cheaprand() % 100
				if u < 9 {
					l.Store(strconv.Itoa(int(runtime_cheaprand())), nil)
				} else if u == 10 {
					l.Delete(strconv.Itoa(int(runtime_cheaprand())))
				} else {
					l.Load(strconv.Itoa(int(runtime_cheaprand())))
				}
			}
		})
	})
}
