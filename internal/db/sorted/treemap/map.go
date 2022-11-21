package treemap

import (
	"fmt"
)

type mapEntry struct {
	left  *mapEntry
	right *mapEntry
	key   any
	value any
}

func (m *mapEntry) String() string {
	return fmt.Sprintf("mapentry: %s %s", m.key, m.value)
}

func newMapEntry[K, V any](k K, v V) *mapEntry {
	return &mapEntry{key: k, value: v}
}

type TreeMap[K any, V any] struct {
	comparator func(a, b K) int
	count      int
	head       *mapEntry
}

func New[K any, V any](comp func(a, b K) int) *TreeMap[K, V] {
	return &TreeMap[K, V]{comparator: comp}
}

func (t *TreeMap[K, V]) search(start *mapEntry, key K) (*mapEntry, bool) {
	if start == nil {
		return nil, false
	}
	switch comp := t.comparator(key, start.key.(K)); comp {
	case -1:
		return t.search(start.left, key)
	case 0:
		return start, true
	case 1:
		return t.search(start.right, key)
	default:
		panic(fmt.Errorf("%T invalid return value from comparator %v", t, comp))
	}
}

func (t *TreeMap[K, V]) Get(key K) (V, bool) {
	if t.Size() == 0 {
		return *new(V), false
	}
	node, ok := t.search(t.head, key)
	if !ok {
		return *new(V), false
	}
	return node.value.(V), true
}

func (t *TreeMap[K, V]) visitInOrder(start *mapEntry, f func(k, v any) bool) {
	if start == nil {
		return
	}
	t.visitInOrder(start.left, f)
	if ok := f(start.key, start.value); !ok {
		return
	}
	t.visitInOrder(start.right, f)
}

func (t *TreeMap[K, V]) Range(f func(k, v any) bool) {
	t.visitInOrder(t.head, f)
}

func (t *TreeMap[K, V]) Remove(_ K) bool {
	return true
}

func (t *TreeMap[K, V]) insert(start *mapEntry, key K, value V) *mapEntry {
	if start == nil {
		t.count++
		return newMapEntry(key, value)
	}
	switch comp := t.comparator(key, start.key.(K)); comp {
	case -1:
		start.left = t.insert(start.left, key, value)
	case 0:
		start.value = value
	case 1:
		start.right = t.insert(start.right, key, value)
	default:
		panic(fmt.Errorf("%T invalid return value from comparator %v", t, comp))
	}
	return start
}

func (t *TreeMap[K, V]) Scan(start K, end K) []V {
	if t.comparator(end, start) < 0 {
		return *new([]V)
	}
	node, ok := t.search(t.head, start)
	if !ok {
		return *new([]V)
	}
	node.left = nil
	out := []V{}
	t.visitInOrder(node, func(k, v any) bool {
		switch comp := t.comparator(k.(K), end); comp {
		case -1:
			out = append(out, v.(V))
		case 0:
			out = append(out, v.(V))
			return false
		}
		return true
	})
	return out
}

func (t *TreeMap[K, V]) Set(key K, value V) {
	t.head = t.insert(t.head, key, value)
}

func (t *TreeMap[K, V]) Size() int {
	return t.count
}

func stringify(start *mapEntry, level int, count int) {
	if count > 20 {
		return
	}
	if start != nil {
		format := ""
		for i := 0; i < level; i++ {
			format += "\t"
		}
		format += "-->"
		level++
		count++
		stringify(start.right, level, count)
		fmt.Printf(format+"%s\n", start.key)
		stringify(start.left, level, count)
	}
}

func (t *TreeMap[K, V]) Print() {
	stringify(t.head, 0, 0)
}
