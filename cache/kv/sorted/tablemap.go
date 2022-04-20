package sorted

import (
	"log"
)

type MapEntry struct {
	Key   any
	Value any
}

func newMapEntry[K any, V any](k K, v V) MapEntry {
	return MapEntry{
		Key:   k,
		Value: v,
	}
}

type MapEntrySlice []MapEntry

type TableMap[K any, V any] struct {
	// Page 430
	impl       []MapEntry
	comparator func(a, b K) int
}

func New[K any, V any](comp func(a, b K) int) *TableMap[K, V] {
	return &TableMap[K, V]{
		impl:       make([]MapEntry, 0, 1024),
		comparator: comp,
	}
}

func (c *TableMap[K, V]) findIndex(key K, low, high int) int {
	if high < low {
		return high + 1
	}
	mid := (low + high) / 2
	entry := c.impl[mid]
	comp := c.comparator(key, entry.Key.(K))
	if comp == 0 {
		// found
		return mid
	} else if comp < 0 {
		// check the lower half of the map
		return c.findIndex(key, low, mid-1)
	} else {
		// check the upper half of the map
		return c.findIndex(key, mid+1, high)
	}
}

// FindIndex finds the index of the key starting
// at the root of the tree
func (c *TableMap[K, V]) FindIndex(key K) int {
	return c.findIndex(key, 0, c.Size()-1)
}

// Size returns the number of entries in the map
func (c *TableMap[K, V]) Size() int {
	return len(c.impl)
}

// Get returns the value associated with the specified key (or else false)
func (c *TableMap[K, V]) Get(key K) (V, bool) {
	j := c.FindIndex(key)
	if j == c.Size() || !c.match(key, j) {
		return *new(V), false
	}
	return c.impl[j].Value.(V), true
}

// Range iterates through each key in the map, applying the range fnc
// to each item. Range fncs should return a bool
// true represents continuation of the range operation
// false indicates ranging should stop
func (c *TableMap[K, V]) Range(fnc func(k any, v any) bool) {
	for _, i := range c.impl {
		result := fnc(i.Key, i.Value)
		if result {
			continue
		}
		return
	}
}

func (c *TableMap[K, V]) match(key K, i int) bool {
	return i < c.Size() && c.comparator(key, c.impl[i].Key.(K)) == 0
}

func (c *TableMap[K, V]) insertSort(index int, el MapEntry) {
	data := append(c.impl, MapEntry{})
	copy(data[index+1:], data[index:])
	data[index] = el
	c.impl = data
}

// Set a key value pair in the map
func (c *TableMap[K, V]) Set(key K, value V) {
	index := c.FindIndex(key)
	if c.match(key, index) {
		c.impl[index] = newMapEntry(key, value)
		return
	}
	c.insertSort(index, newMapEntry(key, value))
}

// Remove removes a key value pair from the map
func (c *TableMap[K, V]) Remove(_ K) (V, bool) {
	return *new(V), false
}

func (c *TableMap[K, V]) Print() {
	log.Printf("%d %v", len(c.impl), c.impl)
}
