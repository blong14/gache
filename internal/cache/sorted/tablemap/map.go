package tablemap

import (
	"fmt"
	"log"
)

type mapEntry struct {
	Key   any
	Value any
}

func newMapEntry[K, V any](k K, v V) *mapEntry {
	return &mapEntry{
		Key:   k,
		Value: v,
	}
}

type TableOption[K, V any] func(t *TableMap[K, V])

type TableMap[K, V any] struct {
	// Page 430
	impl       []*mapEntry
	comparator func(a, b K) int
}

// New returns a newly created TableMap
func New[K, V any](comp func(a, b K) int) *TableMap[K, V] {
	return &TableMap[K, V]{
		impl:       make([]*mapEntry, 0, 1024),
		comparator: comp,
	}
}

// WithCapacity returns a TableOption that
// sets the initial capacity of a given TableMap
func WithCapacity[K, V any](c uint) TableOption[K, V] {
	return func(t *TableMap[K, V]) {
		t.impl = make([]*mapEntry, 0, c)
	}
}

// NewWithOptions returns a newly created TableMap optionally configured
// by a set of TableOption
func NewWithOptions[K, V any](comp func(a, b K) int, options ...TableOption[K, V]) *TableMap[K, V] {
	t := &TableMap[K, V]{
		comparator: comp,
	}
	for _, o := range options {
		o(t)
	}
	return t
}

func (c *TableMap[K, V]) findIndex(key K, low, high int) int {
	if high < low {
		return high + 1
	}
	mid := (low + high) / 2
	entry := c.impl[mid]
	switch comp := c.comparator(key, entry.Key.(K)); comp {
	case -1:
		return c.findIndex(key, low, mid-1)
	case 0:
		return mid
	case 1:
		return c.findIndex(key, mid+1, high)
	default:
		panic(fmt.Errorf("%T invalid return value from comparator %v", c, comp))
	}
}

func (c *TableMap[K, V]) search(key K) int {
	// finds the index of the key starting
	// at the root of the tree
	return c.findIndex(key, 0, c.Size()-1)
}

func (c *TableMap[K, V]) equalto(key K, i uint) bool {
	return i < uint(c.Size()) && c.comparator(key, c.impl[i].Key.(K)) == 0
}

func (c *TableMap[K, V]) greaterthan(key K, i uint) bool {
	return i < uint(c.Size()) && c.comparator(key, c.impl[i].Key.(K)) > 0
}

func (c *TableMap[K, V]) Reset() {
	temp := c.impl[0:]
	c.impl = temp
}

// Get returns the value associated with the specified key (or else false)
func (c *TableMap[K, V]) Get(key K) (V, bool) {
	j := c.search(key)
	if j == c.Size() || !c.equalto(key, uint(j)) {
		return *new(V), false
	}
	return c.impl[j].Value.(V), true
}

func (c *TableMap[K, V]) Print() { log.Printf("%d %v", len(c.impl), c.impl) }

// Range iterates through each key in the map, applying fnc
// to each item. fnc returns a bool
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

// Remove removes a key value pair from the map
func (c *TableMap[K, V]) Remove(_ K) (V, bool) { return *new(V), false }

func (c *TableMap[K, V]) insertLast(el *mapEntry) {
	c.impl = append(c.impl, el)
}

func (c *TableMap[K, V]) insertSort(index int, el *mapEntry) {
	c.insertLast(&mapEntry{})
	copy(c.impl[index+1:], c.impl[index:])
	c.impl[index] = el
}

// Set sets a key value pair in the map
func (c *TableMap[K, V]) Set(key K, value V) {
	if c.greaterthan(key, uint(c.Size()-1)) {
		c.insertLast(newMapEntry(key, value))
		return
	}
	index := c.search(key)
	if c.equalto(key, uint(index)) {
		c.impl[index].Value = value
		return
	}
	c.insertSort(index, newMapEntry(key, value))
}

// Size returns the number of entries in the map
func (c *TableMap[K, V]) Size() int { return len(c.impl) }
