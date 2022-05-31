package treemap_test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
)

func testGetAndSet(t *testing.T) {
	t.Parallel()
	// given
	tree := gtree.New[string, string](strings.Compare)
	expected := "value"
	keys := []string{
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
	for _, key := range keys {
		_, ok := tree.Get(key)
		if ok {
			t.Errorf("key found")
		}
	}

	// when
	for _, key := range keys {
		tree.Set(key, expected)
	}

	// then
	for _, key := range keys {
		actual, ok := tree.Get(key)
		if !ok {
			t.Errorf("key not found")
		}
		if actual != expected {
			t.Errorf("\nwant %s\n got  %s", expected, actual)
		}
	}

	_, ok := tree.Get("missing")
	if ok {
		t.Error("key should be missing")
	}
}

func testRange(t *testing.T) {
	t.Parallel()
	// given
	tree := gtree.New[string, string](strings.Compare)
	expected := []string{
		"key8",
		"key2",
		"key0",
		"key5",
		"key3",
		"key10",
		"key7",
		"key12",
		"key6",
		"key9",
		"key4",
	}
	for i, key := range expected {
		tree.Set(key, fmt.Sprintf("value%d", i))
	}

	// when
	var keys []string
	tree.Range(func(k, _ any) bool {
		keys = append(keys, k.(string))
		return true
	})

	// then
	for _, key := range keys {
		_, ok := tree.Get(key)
		if !ok {
			t.Errorf("%v not found", key)
		}
	}

	fmt.Println(expected)
	tree.Print()
	fmt.Println(keys)
}

func testScan(t *testing.T) {
	t.Parallel()
	// given
	tree := gtree.New[string, string](strings.Compare)
	input := []string{
		"a",
		"b",
		"c",
		"d",
		"e",
		"f",
		"g",
		"h",
		"i",
		"j",
		"k",
	}
	for i, key := range input {
		tree.Set(key, fmt.Sprintf("value%d", i))
	}
	expected := []string{
		"a",
		"b",
		"c",
	}

	// when
	keys := tree.Scan("a", "b")

	// then
	tree.Print()
	fmt.Println(expected)
	fmt.Println(keys)
}

func TestTree(t *testing.T) {
	t.Parallel()

	t.Run("get and insert", testGetAndSet)
	t.Run("range", testRange)
	t.Run("scan", testScan)
}

type bench struct {
	setup    func(*testing.B, *gtree.TreeMap[string, string])
	perG     func(b *testing.B, pb *testing.PB, i int, m *gtree.TreeMap[string, string])
	teardown func(*testing.B, *gtree.TreeMap[string, string]) func()
}

func newMap() *gtree.TreeMap[string, string] {
	return gtree.New[string, string](strings.Compare)
}

func benchMap(b *testing.B, bench bench) {
	b.Run("tree benchmark", func(b *testing.B) {
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
			b.Cleanup(func() {
				bench.teardown(b, m)
			})
		}
	})
}

func BenchmarkConcurrent_LoadMostlyHits(b *testing.B) {
	const hits, misses = 1023, 1

	var mtx sync.RWMutex
	benchMap(b, bench{
		setup: func(_ *testing.B, m *gtree.TreeMap[string, string]) {
			for i := 0; i < hits; i++ {
				mtx.Lock()
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
				mtx.Unlock()
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Range(func(_, _ any) bool { return true })
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gtree.TreeMap[string, string]) {
			for ; pb.Next(); i++ {
				mtx.RLock()
				m.Get(strconv.Itoa(i % (hits + misses)))
				mtx.RUnlock()
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 1023, 1023

	var mtx sync.RWMutex
	benchMap(b, bench{
		setup: func(b *testing.B, m *gtree.TreeMap[string, string]) {
			for i := 0; i < hits; i++ {
				mtx.Lock()
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
				mtx.Unlock()
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Range(func(_, _ any) bool { return true })
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gtree.TreeMap[string, string]) {
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					mtx.RLock()
					if _, ok := m.Get(strconv.Itoa(j)); !ok {
						b.Fatalf("unexpected miss for key %v", j)
					}
					mtx.RUnlock()
				} else {
					mtx.Lock()
					m.Set(strconv.Itoa(j), strconv.Itoa(j))
					mtx.Unlock()
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreCollision(b *testing.B) {
	var mtx sync.RWMutex
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gtree.TreeMap[string, string]) {
			for ; pb.Next(); i++ {
				mtx.Lock()
				m.Set("key", "value")
				mtx.Unlock()
			}
		},
	})
}

func BenchmarkConcurrent_Range(b *testing.B) {
	const mapSize = 1 << 10

	var mtx sync.RWMutex
	benchMap(b, bench{
		setup: func(_ *testing.B, m *gtree.TreeMap[string, string]) {
			for i := 0; i < mapSize; i++ {
				mtx.Lock()
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
				mtx.Unlock()
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gtree.TreeMap[string, string]) {
			for ; pb.Next(); i++ {
				mtx.RLock()
				m.Range(func(_, _ any) bool { return true })
				mtx.RUnlock()
			}
		},
	})
}
