package treemap

// #cgo CFLAGS: -g -Wall
// #include "treemap.h"
import "C"

import (
	"fmt"
)

type mapEntry struct {
	left   *mapEntry
	right  *mapEntry
	parent *mapEntry
	key    any
	value  any
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

func (t *TreeMap[K, V]) xinsert(key K, value V) *mapEntry {
	var y *mapEntry
	for x := t.head; x != nil; {
		y = x
		if t.comparator(key, x.key.(K)) < 0 {
			x = x.left
		} else {
			x = x.right
		}
	}
	if y == nil {
		y = newMapEntry(key, value)
		t.head = y
	} else if t.comparator(key, y.key.(K)) < 0 {
		y.left = newMapEntry(key, value)
	} else {
		y.right = newMapEntry(key, value)
	}
	t.count++
	return y
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
	// t.xinsert(key, value)
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
	fmt.Println(t.Size())
	// fmt.Println("************************************************")
	// stringify(t.head, 0, 0)
	// fmt.Println("************************************************")
}

type CTreeMap struct {
	head  *C.MapEntry
	count int
}

func NewCTreeMap() *CTreeMap {
	return &CTreeMap{}
}

func Get(tm *CTreeMap, key string) (string, bool) {
	k := C.CString(key)
	entry := (*C.MapEntry)(C.get((*C.MapEntry)(tm.head), (*C.char)(k)))
	if entry == nil || entry.value == nil {
		return "", false
	}
	return C.GoString(entry.value), true
}

func Range(tm *CTreeMap, fnc func(k, v any) bool) {

}

func Set(tm *CTreeMap, key, value string) {
	k := C.CString(key)
	v := C.CString(value)
	tm.head = C.set(tm.head, (*C.char)(k), (*C.char)(v))
	tm.count++
}

func Size(tm *CTreeMap) int {
	return tm.count
}
