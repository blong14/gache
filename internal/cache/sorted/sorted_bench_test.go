package sorted_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
	gtable "github.com/blong14/gache/internal/cache/sorted/tablemap"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
)

func newSyncMap(b *testing.B, hits int) *sync.Map {
	b.Helper()
	syncMap := &sync.Map{}
	for i := 0; i < hits; i++ {
		syncMap.Store(strconv.Itoa(i), strconv.Itoa(i))
	}
	return syncMap
}

func newTableMap(b *testing.B, hits int) *gtable.TableMap[string, string] {
	b.Helper()
	tableMap := gtable.New[string, string](strings.Compare)
	for i := 0; i < hits; i++ {
		tableMap.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	return tableMap
}

func newTreeMap(b *testing.B, hits int) *gtree.TreeMap[string, string] {
	b.Helper()
	tree := gtree.New[string, string](strings.Compare)
	for i := 0; i < hits; i++ {
		tree.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	return tree
}

func newSkipList(b *testing.B, hits int) *gskl.SkipList[string, string] {
	b.Helper()
	list := gskl.New[string, string](strings.Compare, strings.EqualFold)
	for i := 0; i < hits; i++ {
		list.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	return list
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
					_, ok := m.Load(strconv.Itoa(j))
					if ok {
						count++
					}
				} else {
					m.Store(strconv.Itoa(j), []byte(strconv.Itoa(j)))
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

func Benchmark_ReadWriteMap(b *testing.B) {
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("map read_%d", i*10), func(b *testing.B) {
			m := make(map[string]struct{})
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				key := strconv.Itoa(rng.Intn(100))
				for pb.Next() {
					if rng.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[key]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[key] = struct{}{}
						mutex.Unlock()
					}
				}
			})
		})

		b.Run(fmt.Sprintf("skl read_%d", i*10), func(b *testing.B) {
			m := gskl.New[[]byte, struct{}](bytes.Compare, bytes.Equal)
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := []byte(strconv.Itoa(rng.Intn(100)))
					if rng.Float32() < readFrac {
						_, ok := m.Get(key)
						if ok {
							count++
						}
					} else {
						m.Set(key, struct{}{})
					}
				}
			})
		})
	}
}
