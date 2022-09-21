//go:build !race

package skiplist_test

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	gcache "github.com/blong14/gache/internal/cache"
	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
)

func TestGetAndSet(t *testing.T) {
	// given
	list := gskl.New[uint64, []byte](gcache.Uint64Compare)
	expected := "value"
	keys := []string{
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
		"l",
		"m",
		"n",
		"o",
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// when
		for i := 0; i < 1000; i++ {
			list.Set(gcache.Hash([]byte(strconv.Itoa(i))), []byte("foo"))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// when
		for _, key := range keys {
			list.Set(gcache.Hash([]byte(key)), []byte(expected))
		}
	}()

	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// then
		for i := 0; i < 1000; i++ {
			if value, ok := list.Get(gcache.Hash([]byte(strconv.Itoa(i)))); !ok {
				t.Errorf("missing key %d %s", i, value)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// then
		for _, key := range keys {
			if value, ok := list.Get(gcache.Hash([]byte(key))); !ok {
				t.Errorf("missing key %s %s", key, value)
			}
		}
	}()

	wg.Wait()
}

type bench struct {
	setup    func(*testing.B, *gskl.SkipList[uint64, []byte])
	perG     func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList[uint64, []byte])
	teardown func(*testing.B, *gskl.SkipList[uint64, []byte]) func()
}

func newSkipList() *gskl.SkipList[uint64, []byte] {
	return gskl.New[uint64, []byte](func(a, b uint64) int {
		switch {
		case a < b:
			return -1
		case a == b:
			return 0
		case a > b:
			return 1
		default:
			panic("error")
		}
	})

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
		setup: func(_ *testing.B, m *gskl.SkipList[uint64, []byte]) {
			for i := 0; i < hits; i++ {
				m.Set(uint64(i), []byte(strconv.Itoa(i)))
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList[uint64, []byte]) {
			for ; pb.Next(); i++ {
				m.Get(uint64(i % (hits + misses)))
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 1023, 1023

	value := []byte("value")
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList[uint64, []byte]) {
			for i := 0; i < hits; i++ {
				m.Set(uint64(i), value)
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList[uint64, []byte]) {
			var count int
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					_, ok := m.Get(uint64(j))
					if ok {
						count++
					}
				} else {
					m.Set(uint64(j), value)
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreCollision(b *testing.B) {
	value := []byte("value")
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList[uint64, []byte]) {
			for ; pb.Next(); i++ {
				m.Set(uint64(i), value)
			}
		},
	})
}
