package tablemap_test

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gtable "github.com/blong14/gache/internal/platform/map/tablemap"
)

func testGetAndSet(t *testing.T) {
	t.Parallel()
	// given
	tree := gtable.NewWithOptions[[]byte, []byte](
		bytes.Compare,
		gtable.WithCapacity[[]byte, []byte](1024),
	)
	start := time.Now()
	count := 5_000
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte(strconv.Itoa(idx))
			tree.Set(key, []byte(fmt.Sprintf("value_%d", idx)))
		}(i)
	}
	wg.Wait()
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := []byte(strconv.Itoa(i))
			if _, ok := tree.Get(k); !ok {
				t.Errorf("missing rawKey %d", i)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("%s", time.Since(start))
}

func testRange(t *testing.T) {
	t.Parallel()
	// given
	tree := gtable.New[string, string](strings.Compare)
	expected := []string{
		"key8",
		"key2",
		"key",
		"key5",
		"key3",
		"key10",
		"key7",
		"key12",
		"key6",
		"key9",
		"key4",
		"-",
	}
	for i, key := range expected {
		tree.Set(key, fmt.Sprintf("value%d", i))
	}

	// when
	var keys []string
	tree.Range(func(k, _ string) bool {
		keys = append(keys, k)
		return true
	})

	// then
	for _, key := range keys {
		_, ok := tree.Get(key)
		if !ok {
			t.Errorf("%v not found", key)
		}
	}
}

func TestTableMap(t *testing.T) {
	t.Parallel()

	t.Run("get and set", testGetAndSet)
	t.Run("range", testRange)
}

type bench struct {
	setup    func(*testing.B, *gtable.TableMap[string, string])
	perG     func(b *testing.B, pb *testing.PB, i int, m *gtable.TableMap[string, string])
	teardown func(*testing.B, *gtable.TableMap[string, string]) func()
}

func newMap() *gtable.TableMap[string, string] {
	return gtable.New[string, string](strings.Compare)
}

func benchMap(b *testing.B, bench bench) {
	b.Run("tablemap benchmark", func(b *testing.B) {
		m := newMap()
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
			b.Cleanup(bench.teardown(b, m))
		}
	})
}

func BenchmarkConcurrent_LoadMostlyHits(b *testing.B) {
	const hits, misses = 1023, 1

	benchMap(b, bench{
		setup: func(_ *testing.B, m *gtable.TableMap[string, string]) {
			for i := 0; i < hits; i++ {
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Range(func(_, _ string) bool { return true })
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gtable.TableMap[string, string]) {
			for ; pb.Next(); i++ {
				m.Get(strconv.Itoa(i % (hits + misses)))
			}
		},
	})

}

func BenchmarkConcurrent_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 1023, 1023

	benchMap(b, bench{
		setup: func(b *testing.B, m *gtable.TableMap[string, string]) {
			for i := 0; i < hits; i++ {
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Range(func(_, _ string) bool { return true })
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gtable.TableMap[string, string]) {
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					if _, ok := m.Get(strconv.Itoa(j)); !ok {
						b.Fatalf("unexpected miss for key %v", j)
					}
				} else {
					m.Set(strconv.Itoa(j), strconv.Itoa(j))
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreCollision(b *testing.B) {
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gtable.TableMap[string, string]) {
			for ; pb.Next(); i++ {
				m.Set("key", "value")
			}
		},
	})
}

func BenchmarkConcurrent_Range(b *testing.B) {
	const mapSize = 1 << 10

	benchMap(b, bench{
		setup: func(_ *testing.B, m *gtable.TableMap[string, string]) {
			for i := 0; i < mapSize; i++ {
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gtable.TableMap[string, string]) {
			for ; pb.Next(); i++ {
				m.Range(func(_, _ string) bool { return true })
			}
		},
	})
}
