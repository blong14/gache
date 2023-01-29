//go:build !jemalloc

package memtable

import (
	"sync/atomic"
	"unsafe"
)

type node struct {
	hash uint64
	next *node
	key  []byte
	val  []byte
}

func newNode(h uint64, k, v []byte, next *node) *node {
	return &node{
		hash: h,
		next: next,
		key:  k,
		val:  v,
	}
}

func freeNode(n *node) {}

func (n *node) Next() *node {
	if n == nil {
		return nil
	}
	return (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&n.next))))
}

type index struct {
	node  *node
	down  *index
	right *index
}

func newIndex(next *node, down, right *index) *index {
	return &index{
		node:  next,
		down:  down,
		right: right,
	}
}

func freeIndex(n *index) {}

func (i *index) Node() *node {
	if i == nil {
		return nil
	}
	return (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&i.node))))
}

func (i *index) Down() *index {
	if i == nil {
		return nil
	}
	return (*index)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&i.down))))
}

func (i *index) Right() *index {
	if i == nil {
		return nil
	}
	return (*index)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&i.right))))
}
