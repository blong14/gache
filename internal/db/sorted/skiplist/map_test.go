package skiplist_test

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	gskl "github.com/blong14/gache/internal/db/sorted/skiplist"
)

type test struct {
	setup    func(*testing.T, *gskl.SkipList)
	run      func(t *testing.T, m *gskl.SkipList)
	teardown func(*testing.T, *gskl.SkipList) func()
}

func testMap(t *testing.T, test test) {
	t.Run("skip list test", func(t *testing.T) {
		t.Parallel()
		m := gskl.New()
		if test.setup != nil {
			test.setup(t, m)
		}
		test.run(t, m)
		if test.teardown != nil {
			t.Cleanup(func() {
				test.teardown(t, m)
			})
		}
	})
}

func TestGetAndSet(t *testing.T) {
	testMap(t, test{
		setup: func(t *testing.T, m *gskl.SkipList) {
			for i := 0; i < 100; i++ {
				m.Set(uint64(i), []byte(fmt.Sprintf("value__%d", i)))
			}
		},
		run: func(t *testing.T, m *gskl.SkipList) {
			for i := 0; i < 100; i++ {
				if _, ok := m.Get(uint64(i)); !ok {
					t.Errorf("missing Key %d", i)
				}
			}
		},
	})
}

type bench struct {
	setup    func(*testing.B, *gskl.SkipList)
	perG     func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList)
	teardown func(*testing.B, *gskl.SkipList) func()
}

func benchMap(b *testing.B, bench bench) {
	b.Run("skip list benchmark", func(b *testing.B) {
		m := gskl.New()
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
		setup: func(_ *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				m.Set(uint64(i), []byte(strconv.Itoa(i)))
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				m.Get(uint64(i % (hits + misses)))
			}
		},
	})
}

func BenchmarkConcurrent_LoadMostlyMisses(b *testing.B) {
	const hits, misses = 1, 1023

	benchMap(b, bench{
		setup: func(_ *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				m.Set(uint64(i), []byte(strconv.Itoa(i)))
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				m.Get(uint64(i % (hits + misses)))
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 1023, 1023

	value := []byte("Value")
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				m.Set(uint64(i), value)
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					_, ok := m.Get(uint64(j))
					if !ok {
						b.Fatalf("unexpected miss for %v", j)
					}
				} else {
					m.Set(uint64(j), value)
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreUnique(b *testing.B) {
	b.Skip("skipping...")
	value := []byte("Value")
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				if _, ok := m.Get(uint64(i)); !ok {
					m.Set(uint64(i), value)
				} else {
					b.Error("unexpected hit")
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreCollision(b *testing.B) {
	const hits = 1023
	value := []byte("Value")
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				m.Set(uint64(i), value)
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				j := i % hits
				if _, ok := m.Get(uint64(j)); ok {
					m.Set(uint64(j), value)
				} else {
					b.Error("unexpected miss")
				}
			}
		},
	})
}

func BenchmarkConcurrent_AdversarialAlloc(b *testing.B) {
	value := []byte("Value")
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			var stores, loadsSinceStore int64
			for ; pb.Next(); i++ {
				m.Get(uint64(i))
				if loadsSinceStore++; loadsSinceStore > stores {
					if _, ok := m.Get(uint64(i)); !ok {
						m.Set(uint64(i), value)
					}
					loadsSinceStore = 0
					stores++
				}
			}
		},
	})
}

func BenchmarkConcurrent_Range(b *testing.B) {
	const mapSize = 1 << 10
	benchMap(b, bench{
		setup: func(_ *testing.B, m *gskl.SkipList) {
			for i := 0; i < mapSize; i++ {
				m.Set(uint64(i), []byte(""))
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				m.Range(func(_ uint64, _ []byte) bool { return true })
			}
		},
	})
}
