package sorted_test

import (
	"bytes"
	"fmt"
	gcache "github.com/blong14/gache/internal/cache"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
)

func newSyncMap(b *testing.B, hits int) *sync.Map {
	b.Helper()
	syncMap := &sync.Map{}
	for i := 0; i < hits; i++ {
		syncMap.Store(strconv.Itoa(i), strconv.Itoa(i))
	}
	return syncMap
}

type bench struct {
	setup    func(*testing.B, *sync.Map)
	perG     func(b *testing.B, pb *testing.PB, i int, m *sync.Map)
	teardown func(*testing.B, *sync.Map)
}

func benchMap(b *testing.B, bench bench) {
	b.Run("sync.Map benchmark", func(b *testing.B) {
		if err := os.Setenv("DEBUG", "false"); err != nil {
			b.Fatal(err)
		}
		m := newSyncMap(b, 0)
		if bench.setup != nil {
			bench.setup(b, m)
		}
		b.ReportAllocs()
		b.ResetTimer()
		var i int64
		b.RunParallel(func(pb *testing.PB) {
			id := int(atomic.AddInt64(&i, 1) - 1)
			bench.perG(b, pb, id*b.N, m)
		})
		if bench.teardown != nil {
			b.Cleanup(func() {
				bench.teardown(b, m)
			})
		}
	})
}

func BenchmarkConcurrent_LoadMostlyHits(b *testing.B) {
	const hits, misses = 1023, 1

	benchMap(b, bench{
		setup: func(_ *testing.B, m *sync.Map) {
			for i := 0; i < hits; i++ {
				m.Store(strconv.Itoa(i), []byte(strconv.Itoa(i)))
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Range(func(_, _ any) bool { return true })
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *sync.Map) {
			for ; pb.Next(); i++ {
				m.Load(strconv.Itoa(i % (hits + misses)))
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 1023, 1023

	value := []byte("value")
	benchMap(b, bench{
		setup: func(b *testing.B, m *sync.Map) {
			for i := 0; i < hits; i++ {
				m.Store(strconv.Itoa(i), []byte(strconv.Itoa(i)))
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Range(func(_, _ any) bool { return true })
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *sync.Map) {
			var count int
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					_, ok := m.Load(j)
					if ok {
						count++
					}
				} else {
					m.Store(j, value)
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreCollision(b *testing.B) {
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *sync.Map) {
			for ; pb.Next(); i++ {
				m.LoadOrStore("key", []byte("value"))
			}
		},
	})
}

func BenchmarkConcurrent_Range(b *testing.B) {
	const mapSize = 1 << 10

	benchMap(b, bench{
		setup: func(_ *testing.B, m *sync.Map) {
			for i := 0; i < mapSize; i++ {
				m.Store(strconv.Itoa(i), []byte(strconv.Itoa(i)))
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *sync.Map) {
			for ; pb.Next(); i++ {
				m.Range(func(_, _ any) bool { return true })
			}
		},
	})
}

func BenchmarkConcurrent_RangeMap(b *testing.B) {
	b.Run("map range", func(b *testing.B) {
		m := make(map[string]struct{})
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		key := strconv.Itoa(rng.Intn(100))
		var mutex sync.RWMutex
		for i := 0; i < 100; i++ {
			mutex.Lock()
			m[key] = struct{}{}
			mutex.Unlock()
		}
		b.ResetTimer()
		var cnt int
		count := make(chan struct{}, 1)
		go func() {
			defer close(count)
			for range count {
				cnt++
			}
		}()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mutex.Lock()
				for range m {
					count <- struct{}{}
				}
				mutex.Unlock()
			}
		})
		b.ReportMetric(float64(cnt), "total")
	})

	b.Run("sync map range", func(b *testing.B) {
		m := sync.Map{}
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < 100; i++ {
			key := strconv.Itoa(rng.Intn(100))
			m.Store(key, struct{}{})
		}
		b.ResetTimer()
		var cnt int
		count := make(chan struct{}, 1)
		go func() {
			defer close(count)
			for range count {
				cnt++
			}
		}()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Range(func(k, v any) bool {
					count <- struct{}{}
					return true
				})
			}
		})
		b.ReportMetric(float64(cnt), "total")
	})

	b.Run("skiplist range", func(b *testing.B) {
		m := gskl.New[[]byte, struct{}](bytes.Compare)
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < 100; i++ {
			key := []byte(strconv.Itoa(rng.Intn(100)))
			m.Set(key, struct{}{})
		}
		b.ResetTimer()
		var cnt int
		count := make(chan struct{}, 1)
		go func() {
			defer close(count)
			for range count {
				cnt++
			}
		}()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.Range(func(k []byte, v struct{}) bool {
					count <- struct{}{}
					return true
				})
			}
		})
		b.ReportMetric(float64(cnt), "total")
	})
}

func BenchmarkConcurrent_ReadWriteMap(b *testing.B) {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0

		b.Run(fmt.Sprintf("map read_%d", i*10), func(b *testing.B) {
			m := make(map[uint64]struct{})
			var mutex sync.RWMutex
			count := make(chan struct{}, 1)
			defer close(count)
			keySet := make(chan uint64, 1)
			var cnt int
			var keys int
			go func() {
				for range count {
					cnt++
				}
			}()
			go func() {
				mKeys := make(map[uint64]bool)
				for key := range keySet {
					if _, ok := mKeys[key]; ok {
						continue
					}
					b.Log(key)
					mKeys[key] = true
					keys++
				}
			}()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := gcache.Hash([]byte(strconv.Itoa(rng.Intn(100))))
					if rng.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[key]
						mutex.RUnlock()
						if ok {
							count <- struct{}{}
						}
					} else {
						mutex.Lock()
						m[key] = struct{}{}
						mutex.Unlock()
						keys++
					}
				}
			})
			b.ReportMetric(float64(cnt), "total")
			b.ReportMetric(float64(keys), "keys")
		})

		b.Run(fmt.Sprintf("sync map read_%d", i*10), func(b *testing.B) {
			m := sync.Map{}
			count := make(chan struct{}, 1)
			defer close(count)
			var cnt int
			go func() {
				for range count {
					cnt++
				}
			}()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := gcache.Hash([]byte(strconv.Itoa(rng.Intn(100))))
					if rng.Float32() < readFrac {
						_, ok := m.Load(key)
						if ok {
							count <- struct{}{}
						}
					} else {
						m.Store(key, struct{}{})
					}
				}
			})
			b.ReportMetric(float64(cnt), "total")
		})

		b.Run(fmt.Sprintf("skl read_%d", i*10), func(b *testing.B) {
			m := gskl.New[uint64, struct{}](gcache.Uint64Compare)
			count := make(chan struct{}, 1)
			defer close(count)
			var cnt int
			go func() {
				for range count {
					cnt++
				}
			}()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := gcache.Hash([]byte(strconv.Itoa(rng.Intn(100))))
					if rng.Float32() < readFrac {
						_, ok := m.Get(key)
						if ok {
							count <- struct{}{}
						}
					} else {
						m.Set(key, struct{}{})
					}
				}
			})
			b.ReportMetric(float64(cnt), "total")
		})
	}
}
