//go:build !race

package skiplist_test

import (
	"bytes"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
)

func TestGetAndSet(t *testing.T) {
	// given
	list := gskl.New[[]byte, []byte](bytes.Compare, bytes.Equal)
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
			list.Set([]byte(strconv.Itoa(i)), []byte("foo"))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// when
		for _, key := range keys {
			list.Set([]byte(key), []byte(expected))
		}
	}()

	time.Sleep(1 * time.Second)
	wg.Wait()
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
	setup    func(*testing.B, *gskl.SkipList[[]byte, []byte])
	perG     func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList[[]byte, []byte])
	teardown func(*testing.B, *gskl.SkipList[[]byte, []byte]) func()
}

func newSkipList() *gskl.SkipList[[]byte, []byte] {
	return gskl.New[[]byte, []byte](bytes.Compare, bytes.Equal)
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
		setup: func(_ *testing.B, m *gskl.SkipList[[]byte, []byte]) {
			for i := 0; i < hits; i++ {
				m.Set([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList[[]byte, []byte]) {
			for ; pb.Next(); i++ {
				m.Get([]byte(strconv.Itoa(i % (hits + misses))))
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreBalanced(b *testing.B) {
	const hits, misses = 1023, 1023

	keys := make([][]byte, 0)
	benchMap(b, bench{
		setup: func(b *testing.B, m *gskl.SkipList[[]byte, []byte]) {
			for i := 0; i < hits; i++ {
				key := []byte(strconv.Itoa(i))
				keys = append(keys, key)
				m.Set(key, key)
			}
		},
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList[[]byte, []byte]) {
			var count int
			for ; pb.Next(); i++ {
				j := i % (hits + misses)
				if j < hits {
					_, ok := m.Get(keys[j])
					if ok {
						count++
					}
				} else {
					m.Set(keys[j], keys[j])
				}
			}
		},
	})
}

func BenchmarkConcurrent_LoadOrStoreCollision(b *testing.B) {
	key, value := []byte("key"), []byte("value")
	benchMap(b, bench{
		perG: func(b *testing.B, pb *testing.PB, i int, m *gskl.SkipList[[]byte, []byte]) {
			for ; pb.Next(); i++ {
				m.Set(key, value)
			}
		},
	})
}
