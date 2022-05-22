package skiplist_test

import (
	"bytes"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	glist "github.com/blong14/gache/internal/cache/sorted/skiplist"
)

func TestGetAndSet(t *testing.T) {
	t.Parallel()
	// given
	list := glist.New[[]byte, []byte](bytes.Compare)
	expected := "value"
	keys := []string{
		"key8",
		"key2",
		"key1",
		"key5",
		"key3",
		"key9",
		"key7",
		"key4",
		"key6",
	}

	done := make(chan struct{})
	go func() {
		// when
		for _, key := range keys {
			list.Set([]byte(key), []byte(expected))
		}
		done <- struct{}{}
	}()

	<-done
	for _, key := range keys {
		if value, ok := list.Get([]byte(key)); !ok {
			t.Errorf("missing key %s %s", key, value)
		}
	}
	list.Print()
	_, ok := list.Get([]byte("missing"))
	if ok {
		t.Error("key should be missing")
	}
}

type bench struct {
	setup    func(*testing.B, *glist.SkipList[string, string])
	perG     func(b *testing.B, pb *testing.PB, i int, m *glist.SkipList[string, string])
	teardown func(*testing.B, *glist.SkipList[string, string]) func()
}

func newSkipList() *glist.SkipList[string, string] {
	return glist.New[string, string](strings.Compare)
}

func benchMap(b *testing.B, bench bench) {
	b.Run("skip list benchmark", func(b *testing.B) {
		m := newSkipList()
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
		setup: func(_ *testing.B, m *glist.SkipList[string, string]) {
			for i := 0; i < hits; i++ {
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Range(func(_, _ any) bool { return true })
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *glist.SkipList[string, string]) {
			for ; pb.Next(); i++ {
				m.Get(strconv.Itoa(i % (hits + misses)))
			}
		},
	})

}

func BenchmarkConcurrent_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 128, 128

	benchMap(b, bench{
		setup: func(b *testing.B, m *glist.SkipList[string, string]) {
			for i := 0; i < hits; i++ {
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
			}
			// Prime the map to get it into a steady state.
			for i := 0; i < hits*2; i++ {
				m.Range(func(_, _ any) bool { return true })
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *glist.SkipList[string, string]) {
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					if _, ok := m.Get(strconv.Itoa(j)); !ok {
						b.Fatalf("unexpected miss for key %v", j)
					}
				} else {
					m.Set(strconv.Itoa(i), strconv.Itoa(i))
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreCollision(b *testing.B) {
	benchMap(b, bench{
		setup: func(_ *testing.B, m *glist.SkipList[string, string]) {
			m.Set("key", "value")
		},

		perG: func(b *testing.B, pb *testing.PB, i int, m *glist.SkipList[string, string]) {
			for ; pb.Next(); i++ {
				m.Set("key", "value")
			}
		},
	})
}

func BenchmarkConcurrent_Range(b *testing.B) {
	const mapSize = 1 << 10

	benchMap(b, bench{
		setup: func(_ *testing.B, m *glist.SkipList[string, string]) {
			for i := 0; i < mapSize; i++ {
				m.Set(strconv.Itoa(i), strconv.Itoa(i))
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *glist.SkipList[string, string]) {
			for ; pb.Next(); i++ {
				m.Range(func(_, _ any) bool { return true })
			}
		},
	})
}
