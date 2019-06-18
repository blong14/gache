package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type (
	// CacheKey used to index a CacheValue
	CacheKey interface{}

	// CacheValue represents the data that is cached
	CacheValue interface{}

	// Cachable represents the data to cache
	Cachable interface{}
)

// Cache represents a generic cache implementation
type Cache struct {
	cache  map[CacheKey]map[string]CacheValue
	reload func() Cachable
	ttl    time.Duration
	mutex sync.RWMutex
}

// Get a value from the cache
func (c *Cache) Get(key CacheKey) (CacheValue, error) {
	c.mutex.RLock()
	value, ok := c.cache[key]
	c.mutex.RUnlock()
	if !ok {
		return nil, errors.New("missing value")
	}
	if time.Now().UnixNano() > value["expire"].(int64) {
		data := c.reload()
		c.Put(key, data)
		return data, nil
	}
	return value["data"], nil
}

// Put a value into the cache
func (c *Cache) Put(key CacheKey, value CacheValue) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	expire := time.Now().Add(c.ttl).UnixNano()
	c.cache[key] = map[string]CacheValue{"data": value, "expire": expire}
}

// NewCache seeds and returns a Cache
func NewCache(f func() Cachable, timeout time.Duration) *Cache {
	return &Cache{
		cache:  make(map[CacheKey]map[string]CacheValue),
		reload: f,
		ttl:    timeout,
	}
}

func main() {
	fmt.Println("main method")
}
