package sorted_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
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
	list := gskl.New[string, string](strings.Compare)
	for i := 0; i < hits; i++ {
		list.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	return list
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
