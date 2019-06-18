package main

import (
	"fmt"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	timeout := 20 * time.Second
	c := NewCache(func() Cachable {
		d := time.Now().UnixNano()
		return fmt.Sprintf("input %d", d)
	}, timeout)

	c.Put("data", "some data")
	c.Put("data2", "some other data")

	if data, err := c.Get("data"); err == nil {
		if data != "some data" {
			t.Fail()
		}
	}

	if data, err := c.Get("data2"); err == nil {
		if data != "some other data" {
			t.Fail()
		}
	}
}

func TestCacheTimeout(t *testing.T) {
	timeout := 1 * time.Second

	c := NewCache(func() Cachable {
		return "fresh data"
	}, timeout)

	c.Put("data", "expired data")

	wait := 2 * time.Second
	time.Sleep(wait)

	if data, err := c.Get("data"); err != nil {
		if data != "fresh data" {
			t.Fail()
		}
	}
}

func TestCacheMissingKey(t *testing.T) {
	timeout := 1 * time.Second

	c := NewCache(func() Cachable {
		return "data"
	}, timeout)

	c.Put("data", "data")

	if _, ok := c.Get("fooo"); ok == nil {
		t.Fail()
	}
}

func TestCache_MultipleGoRoutines(t *testing.T) {
	timeout := 1 * time.Minute

	c := NewCache(func() Cachable {
		return "data"
	}, timeout)


	go (func() {
		c.Put("data1", "some data")
		wait := 2 * time.Second
		time.Sleep(wait)
		if data, err := c.Get("data1"); err != nil {
			if data != "some data" {
				t.Fail()
			}
		}
	})()

	go (func() {
		c.Put("data1", "some more data")
		if data, err := c.Get("data1"); err != nil {
			if data != "some more data" {
				t.Fail()
			}
		}
	})()
}

