package tablemap

import (
	"fmt"
	"log"
	"sync"
)

type MapEntry struct {
	Key   any
	Value any
}

func newMapEntry[K, V any](k K, v V) *MapEntry {
	return &MapEntry{
		Key:   k,
		Value: v,
	}
}

type TableOption[K, V any] func(t *TableMap[K, V])

type TableMap[K, V any] struct {
	// Page 430
	mtx        sync.RWMutex
	impl       []*MapEntry
	comparator func(a, b K) int
}

// New returns a newly created TableMap
func New[K, V any](comp func(a, b K) int) *TableMap[K, V] {
	return &TableMap[K, V]{
		mtx:        sync.RWMutex{},
		impl:       make([]*MapEntry, 0, 1024),
		comparator: comp,
	}
}

// WithCapacity returns a TableOption that
// sets the initial capacity of a given TableMap
func WithCapacity[K, V any](c uint) TableOption[K, V] {
	return func(t *TableMap[K, V]) {
		t.impl = make([]*MapEntry, 0, c)
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

func (c *TableMap[K, V]) Init(comp func(a, b K) int) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.impl = make([]*MapEntry, 0, 1024)
	c.comparator = comp
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
	return c.findIndex(key, 0, c.size()-1)
}

func (c *TableMap[K, V]) equalto(key K, i uint) bool {
	return i < uint(c.size()) && c.comparator(key, c.impl[i].Key.(K)) == 0
}

func (c *TableMap[K, V]) greaterthan(key K, i uint) bool {
	return i < uint(c.size()) && c.comparator(key, c.impl[i].Key.(K)) > 0
}

func (c *TableMap[K, V]) Reset() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	temp := c.impl[0:]
	c.impl = temp
}

// Get returns the value associated with the specified key (or else false)
func (c *TableMap[K, V]) Get(key K) (V, bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	j := c.search(key)
	if j == c.size() || !c.equalto(key, uint(j)) {
		return *new(V), false
	}
	return c.impl[j].Value.(V), true
}

func (c *TableMap[K, V]) Print() {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	log.Printf("%d %v", len(c.impl), c.impl)
}

// Range iterates through each key in the map, applying fnc
// to each item. fnc returns a bool
// true represents continuation of the range operation
// false indicates ranging should stop
func (c *TableMap[K, V]) Range(fnc func(k K, v V) bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	for _, i := range c.impl {
		ok := fnc(i.Key.(K), i.Value.(V))
		if !ok {
			return
		}
	}
}

// Remove removes a key value pair from the map
func (c *TableMap[K, V]) Remove(_ K) (V, bool) { return *new(V), false }

func (c *TableMap[K, V]) insertLast(el *MapEntry) {
	c.impl = append(c.impl, el)
}

func (c *TableMap[K, V]) insertSort(index int, el *MapEntry) {
	c.insertLast(&MapEntry{})
	copy(c.impl[index+1:], c.impl[index:])
	c.impl[index] = el
}

// Set sets a key value pair in the map
func (c *TableMap[K, V]) Set(key K, value V) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.greaterthan(key, uint(c.size()-1)) {
		c.insertLast(newMapEntry(key, value))
		return
	}
	index := c.search(key)
	if c.equalto(key, uint(index)) {
		c.impl[index] = newMapEntry(key, value)
		return
	}
	c.insertSort(index, newMapEntry(key, value))
}

func (c *TableMap[K, V]) size() int {
	return len(c.impl)
}

// Size returns the number of entries in the map
func (c *TableMap[K, V]) Size() int {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.size()
}
