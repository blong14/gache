package skiplist_test

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	gskl "github.com/blong14/gache/internal/map/skiplist"
)

type test struct {
	setup    func(*testing.T, *gskl.SkipList)
	run      func(t *testing.T, m *gskl.SkipList)
	teardown func(*testing.T, *gskl.SkipList) func()
}

func testMap(t *testing.T, name string, test test) {
	t.Run(fmt.Sprintf("skip list test %s", name), func(t *testing.T) {
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

func TestHeight(t *testing.T) {
	expected := 20
	testMap(t, "height", test{
		setup: func(t *testing.T, m *gskl.SkipList) {
			for i := 0; i < expected; i++ {
				err := m.Set(
					[]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value__%d", i)))
				if err != nil {
					t.Fail()
				}
			}
		},
		run: func(t *testing.T, m *gskl.SkipList) {
			actual := m.Height()
			if actual > uint64(expected) {
				t.Errorf("w %d g %d", expected, actual)
			}
		},
	})
}

func TestCount(t *testing.T) {
	expected := 100
	testMap(t, "count", test{
		setup: func(t *testing.T, m *gskl.SkipList) {
			for i := 0; i < expected; i++ {
				err := m.Set(
					[]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value__%d", i)))
				if err != nil {
					t.Fail()
				}
			}
		},
		run: func(t *testing.T, m *gskl.SkipList) {
			actual := m.Count()
			if actual != uint64(expected) {
				t.Errorf("w %d g %d", expected, actual)
			}
		},
	})
}

func TestGetAndSet(t *testing.T) {
	count := 100
	testMap(t, "get and set", test{
		run: func(t *testing.T, m *gskl.SkipList) {
			for i := 0; i < count; i++ {
				err := m.Set(
					[]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value__%d", i)))
				if err != nil {
					t.Error(err)
				}
			}
			for i := 0; i < count; i++ {
				if _, ok := m.Get([]byte(fmt.Sprintf("key_%d", i))); !ok {
					t.Errorf("missing key %d", i)
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
		setup: func(b *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				v := strconv.Itoa(i)
				err := m.Set([]byte(v), []byte(v))
				if err != nil {
					b.Fail()
				}
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				m.Get([]byte(strconv.Itoa(i % (hits + misses))))
			}
		},
	})
}

func BenchmarkConcurrent_LoadMostlyMisses(b *testing.B) {
	const hits, misses = 1, 1023

	benchMap(b, bench{
		setup: func(_ *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				v := strconv.Itoa(i)
				err := m.Set([]byte(v), []byte(v))
				if err != nil {
					b.Fail()
				}
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				m.Get([]byte(strconv.Itoa(i % (hits + misses))))
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 1023, 1023

	value := []byte("value")
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				v := strconv.Itoa(i)
				err := m.Set([]byte(v), value)
				if err != nil {
					b.Fail()
				}
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				v := strconv.Itoa(j)
				if j < hits {
					_, ok := m.Get([]byte(v))
					if !ok {
						b.Fatalf("unexpected miss for %v", j)
					}
				} else {
					if err := m.Set([]byte(v), value); err != nil {
						b.Error(err)
					}
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreUnique(b *testing.B) {
	b.Skip("skipping...")
	value := []byte("value")
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				v := strconv.Itoa(i)
				if _, ok := m.Get([]byte(v)); !ok {
					err := m.Set([]byte(v), value)
					if err != nil {
						b.Error(err)
					}
				} else {
					b.Error("unexpected hit")
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreCollision(b *testing.B) {
	const hits = 1023
	value := []byte("value")
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				v := strconv.Itoa(i)
				err := m.Set([]byte(v), value)
				if err != nil {
					b.Fail()
				}
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				j := i % hits
				v := strconv.Itoa(j)
				if _, ok := m.Get([]byte(v)); ok {
					err := m.Set([]byte(v), value)
					if err != nil {
						b.Error(err)
					}
				} else {
					b.Error("unexpected miss")
				}
			}
		},
	})
}

func BenchmarkConcurrent_AdversarialAlloc(b *testing.B) {
	value := []byte("value")
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			var stores, loadsSinceStore int64
			for ; pb.Next(); i++ {
				v := strconv.Itoa(i)
				m.Get([]byte(v))
				if loadsSinceStore++; loadsSinceStore > stores {
					if _, ok := m.Get([]byte(v)); !ok {
						err := m.Set([]byte(v), value)
						if err != nil {
							b.Error(err)
						}
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
				v := strconv.Itoa(i)
				err := m.Set([]byte(v), []byte(""))
				if err != nil {
					b.Fail()
				}
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				m.Range(func(_, _ []byte) bool { return true })
			}
		},
	})
}
