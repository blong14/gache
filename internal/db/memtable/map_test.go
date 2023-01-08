package memtable_test

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gskl "github.com/blong14/gache/internal/db/memtable"
)

type test struct {
	setup    func(*testing.T, *gskl.SkipList)
	run      func(t *testing.T, m *gskl.SkipList)
	teardown func(*testing.T, *gskl.SkipList) func()
}

func testMap(t *testing.T, name string, test test) {
	t.Run(fmt.Sprintf("skip list test %s", name), func(t *testing.T) {
		t.Parallel()
		m := gskl.NewSkipList()
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
	count := 50_000
	testMap(t, "get and set", test{
		run: func(t *testing.T, m *gskl.SkipList) {
			start := time.Now()
			var wg sync.WaitGroup
			for i := 0; i < count; i++ {
				wg.Add(1)
				go func(indx int) {
					defer wg.Done()
					k := []byte(fmt.Sprintf("key-%d", indx))
					err := m.Set(k, []byte(fmt.Sprintf("value__%d", indx)))
					if err != nil {
						t.Error(err)
					}
				}(i)
			}
			wg.Wait()
			t.Logf("%s", time.Since(start))
			for i := 0; i < count; i++ {
				wg.Add(1)
				go func(indx int) {
					defer wg.Done()
					k := []byte(fmt.Sprintf("key-%d", indx))
					if _, ok := m.Get(k); !ok {
						t.Errorf("missing rawKey %d", indx)
					}
				}(i)
			}
			wg.Wait()
			// m.Print()
			t.Logf("%s", time.Since(start))
		},
	})
}

func TestRange(t *testing.T) {
	t.Skip("skipping...")
	expected := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	testMap(t, "count", test{
		setup: func(t *testing.T, m *gskl.SkipList) {
			for _, i := range expected {
				err := m.Set(i, i)
				if err != nil {
					t.Fail()
				}
			}
		},
		run: func(t *testing.T, m *gskl.SkipList) {
			actual := make([][]byte, 0)
			m.Range(func(k, _ []byte) bool {
				actual = append(actual, k)
				return true
			})
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("w %v g %v", expected, actual)
			}
		},
	})
}

func TestScan(t *testing.T) {
	// t.Skip("skipping...")
	expected := [][]byte{[]byte("aaaa"), []byte("bbbb"), []byte("cccc"), []byte("dddd"), []byte("eeee")}
	testMap(t, "scan", test{
		setup: func(t *testing.T, m *gskl.SkipList) {
			for _, i := range expected {
				err := m.Set(i, i)
				if err != nil {
					t.Fail()
				}
			}
		},
		run: func(t *testing.T, m *gskl.SkipList) {
			actual := make([][]byte, 0)
			m.Scan(expected[1], expected[4], func(k, _ []byte) bool {
				actual = append(actual, k)
				return true
			})
			if !reflect.DeepEqual(actual, expected[1:]) {
				t.Errorf("w %v g %v", expected[1:], actual)
			}
			actual = make([][]byte, 0)
			m.Scan(expected[0], nil, func(k, _ []byte) bool {
				actual = append(actual, k)
				return true
			})
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("w %v g %v", expected, actual)
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
		m := gskl.NewSkipList()
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

func BenchmarkSkiplist_LoadMostlyHits(b *testing.B) {
	const hits, misses = 1023, 1
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				key := []byte(strconv.Itoa(i))
				if err := m.Set(key, key); err != nil {
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

func BenchmarkSkiplist_LoadMostlyMisses(b *testing.B) {
	const hits, misses = 1, 1023
	benchMap(b, bench{
		setup: func(_ *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				key := []byte(strconv.Itoa(i))
				if err := m.Set(key, key); err != nil {
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

func BenchmarkSkiplist_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 1023, 1023
	value := []byte("value")
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				key := []byte(strconv.Itoa(i))
				if err := m.Set(key, value); err != nil {
					b.Fail()
				}
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				key := []byte(strconv.Itoa(j))
				if j < hits {
					if _, ok := m.Get(key); !ok {
						b.Fatalf("unexpected miss for %v", j)
					}
				} else {
					if err := m.Set(key, value); err != nil {
						b.Error(err)
					}
				}
			}
		},
	})
}

func BenchmarkSkiplist_LoadOrStoreUnique(b *testing.B) {
	const hits = 1023
	value := []byte("value")
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				j := i % hits
				key := []byte(strconv.Itoa(j))
				if _, ok := m.Get(key); !ok {
					if err := m.Set(key, value); err != nil {
						b.Error(err)
					}
				}
			}
		},
	})
}

func BenchmarkSkiplist_LoadOrStoreCollision(b *testing.B) {
	const hits = 1023
	value := []byte("value")
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList) {
			for i := 0; i < hits; i++ {
				key := []byte(strconv.Itoa(i))
				if err := m.Set(key, value); err != nil {
					b.Fail()
				}
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			for ; pb.Next(); i++ {
				j := i % hits
				key := []byte(strconv.Itoa(j))
				if _, ok := m.Get(key); ok {
					if err := m.Set(key, value); err != nil {
						b.Error(err)
					}
				} else {
					b.Errorf("unexpected miss %s", key)
				}
			}
		},
	})
}

func BenchmarkSkiplist_AdversarialAlloc(b *testing.B) {
	value := []byte("value")
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList) {
			var stores, loadsSinceStore int64
			for ; pb.Next(); i++ {
				key := []byte(strconv.Itoa(i))
				m.Get(key)
				if loadsSinceStore++; loadsSinceStore > stores {
					if _, ok := m.Get(key); !ok {
						err := m.Set(key, value)
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

func BenchmarkSkiplist_Range(b *testing.B) {
	const mapSize = 1 << 10
	value := []byte("")
	benchMap(b, bench{
		setup: func(_ *testing.B, m *gskl.SkipList) {
			for i := 0; i < mapSize; i++ {
				key := []byte(strconv.Itoa(i))
				err := m.Set(key, value)
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
