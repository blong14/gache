package _map

import (
	"sync"
)

type Map[K any, V any] struct {
	impl sync.Map
}

func New[K any, V any]() *Map[K, V] {
	return &Map[K, V]{impl: sync.Map{}}
}

func (c *Map[K, V]) Get(key K) (V, bool) {
	value, ok := c.impl.Load(key)
	return value.(V), ok
}

func (c *Map[K, V]) Remove(k K) (V, bool) {
	v, ok := c.impl.LoadAndDelete(k)
	return v.(V), ok
}

func (c *Map[K, V]) Set(key K, value V)                { c.impl.Store(key, value) }
func (c *Map[K, V]) Range(fnc func(k any, v any) bool) { c.impl.Range(fnc) }
