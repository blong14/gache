//go:build jemalloc
// +build jemalloc

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

var nodeSz = int(unsafe.Sizeof(node{}))

func newNode(h uint64, k, v []byte, next *node) *node {
	b := Calloc(nodeSz)
	n := (*node)(unsafe.Pointer(&b[0]))
	n.hash = h
	n.key = k
	n.val = v
	n.next = next
	return n
}

func freeNode(n *node) {
	buf := (*[MaxArrayLen]byte)(unsafe.Pointer(n))[:nodeSz:nodeSz]
	Free(buf)
}

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

var indexSz = int(unsafe.Sizeof(index{}))

func newIndex(next *node, down, right *index) *index {
	b := Calloc(indexSz)
	n := (*index)(unsafe.Pointer(&b[0]))
	n.node = next
	n.down = down
	n.right = right
	return n
}

func freeIndex(n *index) {
	buf := (*[MaxArrayLen]byte)(unsafe.Pointer(n))[:indexSz:indexSz]
	Free(buf)
}

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
