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

func BenchmarkSorted_InsertInOrder(b *testing.B) {
	hits := 100_000
	b.Run(fmt.Sprintf("TreeMap_%d", hits), func(b *testing.B) {
		b.ReportAllocs()
		treeMap := newTreeMap(b, 0)
		for i := 0; i < b.N; i++ {
			for j := 0; j < hits; j++ {
				treeMap.Set(strconv.Itoa(j), strconv.Itoa(j))
			}
		}
	})

	b.Run(fmt.Sprintf("SkipList%d", hits), func(b *testing.B) {
		b.ReportAllocs()
		list := newSkipList(b, 0)
		for i := 0; i < b.N; i++ {
			for j := 0; j < hits; j++ {
				list.Set(strconv.Itoa(j), strconv.Itoa(j))
			}
		}
	})

	b.Run(fmt.Sprintf("TableMap%d", hits), func(b *testing.B) {
		b.ReportAllocs()
		tableMap := newTableMap(b, 0)
		for i := 0; i < b.N; i++ {
			for j := 0; j < hits; j++ {
				tableMap.Set(strconv.Itoa(j), strconv.Itoa(j))
			}
		}
	})

	b.Run(fmt.Sprintf("SyncMap%d", hits), func(b *testing.B) {
		b.ReportAllocs()
		syncMap := newSyncMap(b, 0)
		for i := 0; i < b.N; i++ {
			for j := 0; j < hits; j++ {
				syncMap.Store(strconv.Itoa(j), strconv.Itoa(j))
			}
		}
	})
}

func BenchmarkSorted_InsertRandom(b *testing.B) {
	hits := 100_000
	random := func(numInts int) []int {
		source := rand.NewSource(time.Now().UnixNano())
		generator := rand.New(source)
		result := make([]int, numInts)
		for i := 0; i < numInts; i++ {
			result[i] = generator.Intn(numInts * 100)
		}
		return result
	}
	input := random(hits)

	b.Run(fmt.Sprintf("TreeMap_%d", hits), func(b *testing.B) {
		b.ReportAllocs()
		treeMap := newTreeMap(b, 0)
		for i := 0; i < b.N; i++ {
			for _, j := range input {
				treeMap.Set(strconv.Itoa(j), strconv.Itoa(j))
			}
		}
	})

	b.Run(fmt.Sprintf("SkipList%d", hits), func(b *testing.B) {
		b.ReportAllocs()
		list := newSkipList(b, 0)
		for i := 0; i < b.N; i++ {
			for _, j := range input {
				list.Set(strconv.Itoa(j), strconv.Itoa(j))
			}
		}
	})

	b.Run(fmt.Sprintf("TableMap%d", hits), func(b *testing.B) {
		b.ReportAllocs()
		tableMap := newTableMap(b, 0)
		for i := 0; i < b.N; i++ {
			for _, j := range input {
				tableMap.Set(strconv.Itoa(j), strconv.Itoa(j))
			}
		}
	})

	b.Run(fmt.Sprintf("SyncMap%d", hits), func(b *testing.B) {
		b.ReportAllocs()
		syncMap := newSyncMap(b, 0)
		for i := 0; i < b.N; i++ {
			for j := 0; j < hits; j++ {
				syncMap.Store(strconv.Itoa(j), strconv.Itoa(j))
			}
		}
	})
}

func BenchmarkSorted_Append(b *testing.B) {
	for _, hits := range []int{100_000} {
		b.Run(fmt.Sprintf("TreeMap_%d", hits), func(b *testing.B) {
			b.ReportAllocs()
			treeMap := newTreeMap(b, hits)
			for i := 0; i < b.N; i++ {
				treeMap.Set("99999", "99")
			}
		})

		b.Run(fmt.Sprintf("SkipList%d", hits), func(b *testing.B) {
			b.ReportAllocs()
			skl := newSkipList(b, hits)
			for i := 0; i < b.N; i++ {
				skl.Set("99999", "99")
			}
		})

		b.Run(fmt.Sprintf("TableMap%d", hits), func(b *testing.B) {
			b.ReportAllocs()
			tableMap := newTableMap(b, hits)
			for i := 0; i < b.N; i++ {
				tableMap.Set("99999", "99")
			}
		})

		b.Run(fmt.Sprintf("SyncMap%d", hits), func(b *testing.B) {
			b.ReportAllocs()
			syncMap := newSyncMap(b, hits)
			for i := 0; i < b.N; i++ {
				syncMap.Store("99999", "99")
			}
		})
	}
}

func BenchmarkSorted_GetRandom(b *testing.B) {
	hits := 100_000
	input := []string{"0", "25000", "50000", "75000", "99999"}

	treeMap := newTreeMap(b, hits)
	for _, key := range input {
		b.Run(fmt.Sprintf("TreeMap_%s", key), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if value, ok := treeMap.Get(key); !ok {
					b.Errorf("want %s got %s", key, value)
				}
			}
		})
	}

	skl := newSkipList(b, hits)
	for _, key := range input {
		b.Run(fmt.Sprintf("SkipList%s", key), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if value, ok := skl.Get(key); !ok {
					b.Errorf("want %s got %s", key, value)
				}
			}
		})
	}

	tableMap := newTableMap(b, hits)
	for _, key := range input {
		b.Run(fmt.Sprintf("TableMap_%s", key), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if value, ok := tableMap.Get(key); !ok {
					b.Errorf("want %s got %s", key, value)
				}
			}
		})
	}

	syncMap := newSyncMap(b, hits)
	for _, key := range input {
		b.Run(fmt.Sprintf("SyncMap_%s", key), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if value, ok := syncMap.Load(key); !ok {
					b.Errorf("want %s got %s", key, value)
				}
			}
		})
	}
}

func BenchmarkConcurrent_ReadWriteMap(b *testing.B) {
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
