package xskiplist

import (
	"errors"
	"fmt"
	"hash/maphash"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	_ "unsafe"
)

const MaxLevels = 24

type unlink int

var seed = maphash.MakeSeed()

func Hash(key []byte) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	_, _ = h.Write(key)
	return h.Sum64()
}

// Uint32 returns a lock free uint32 value.
//
//go:linkname Uint32 runtime.fastrand
func Uint32() uint32

// Uint32n returns a lock free uint32 value in the interval [0, n).
//
//go:linkname Uint32n runtime.fastrandn
func Uint32n(n uint32) uint32

const (
	ForceUnlink unlink = iota
	AssistUnlink
	DontUnlink
)

type markable struct {
	marked bool
	next   *node
}

func (m *markable) String() string {
	return fmt.Sprintf("[%s]", m.next)
}

func stripMark(x *markable) *markable {
	if x == nil {
		return nil
	}
	return &markable{
		next: x.next,
	}
}

func hasMark(x *markable) bool {
	if x == nil {
		return false
	}
	return x.marked
}

func getNode(x *markable) *node {
	if x == nil {
		return nil
	}
	return x.next
}

type node struct {
	key       []byte
	val       []byte
	index     uint64
	numLevels uint
	next      []*markable
}

func (n *node) reset() {
	n.key = nil
	n.val = nil
	n.numLevels = 0
	n.next = nil
	n.index = 0
}

func (n *node) String() string {
	return fmt.Sprintf("[%s:%s]", string(n.key), string(n.val))
}

func newNode(k, v []byte, levels uint) *node {
	return &node{
		key:       k,
		val:       v,
		numLevels: levels,
		next:      make([]*markable, MaxLevels),
	}
}

type SkipList struct {
	head      *node
	highWater uint32
}

func New() *SkipList {
	return &SkipList{
		head:      newNode(nil, nil, MaxLevels),
		highWater: 1,
	}
}

func (sk *SkipList) randomLevel() uint64 {
	levels := Uint32n(MaxLevels + 1)
	if levels == 0 {
		return 1
	}
	return uint64(levels)
}

var itemPool = sync.Pool{New: func() interface{} { return new(node) }}

func (sk *SkipList) findPreds(preds, succs []*node, n int, key uint64, link unlink) *node {
	d := -1
	i := itemPool.Get()
	defer itemPool.Put(i)
	item := i.(*node)
	item.reset()
	pred := sk.head
	highWater := atomic.LoadUint32(&sk.highWater)
	for level := int(highWater - 1); level >= 0; level-- {
		curr := pred.next[level]
		if curr == nil && level >= n {
			continue
		}
		if hasMark(curr) {
			return sk.findPreds(preds, succs, n, key, link) // retry
		}
		item = getNode(curr)
		for item != nil {
			curr = item.next[level]
			for hasMark(curr) {
				if link == DontUnlink {
					m := stripMark(curr)
					if m == nil {
						break
					}
					item = getNode(m)
					if item == nil {
						break
					}
					curr = item.next[level]
				} else {
					m := stripMark(curr)
					other := (*markable)(atomic.SwapPointer(
						(*unsafe.Pointer)(unsafe.Pointer(&pred.next[level])),
						unsafe.Pointer(m),
					))
					if other == curr {
						m = stripMark(curr)
						if m != nil {
							item = getNode(m)
						}
					} else {
						if hasMark(other) {
							return sk.findPreds(preds, succs, n, key, link)
						}
						item = getNode(other)
					}
					if item != nil {
						curr = item.next[level]
					} else {
						curr = nil
					}
				}
			}
			if item == nil {
				break
			}
			if key > item.index {
				d = 1
			} else if key == item.index {
				d = 0
			} else {
				d = -1
				break
			}
			//d = bytes.Compare(key, item.key) // a > b = 1
			//if d < 0 {
			//	break
			//}
			if d == 0 && link != ForceUnlink {
				break
			}
			pred = item
			item = getNode(curr)
		}
		if level < n {
			if preds != nil {
				preds[level] = pred
			}
			if succs != nil {
				succs[level] = item
			}
		}
	}
	if d == 0 {
		return item
	}
	return nil
}

func (sk *SkipList) updateItem(item *node, newVal []byte) ([]byte, bool) {
	for {
		oldVal := item.val
		if oldVal == nil {
			return nil, false
		}
		if atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&item.val)),
			unsafe.Pointer(&oldVal),
			unsafe.Pointer(&newVal),
		) {
			return newVal, true
		}
	}
}

func (sk *SkipList) cas(key, newVal []byte) ([]byte, bool) {
	if len(newVal) == 0 {
		return nil, false
	}
	n := sk.randomLevel()
	index := Hash(key)
	preds := make([]*node, MaxLevels)
	nexts := make([]*node, MaxLevels)
	oldItem := sk.findPreds(preds, nexts, int(n), index, AssistUnlink)
	if oldItem != nil {
		retVal, ok := sk.updateItem(oldItem, newVal)
		if retVal != nil && ok {
			return retVal, ok
		}
		return sk.cas(key, newVal)
	}
	newItem := newNode(key, newVal, uint(n))
	newItem.index = index
	for level := 0; level < int(newItem.numLevels); level++ {
		n := nexts[level]
		if n != nil {
			newItem.next[level] = &markable{next: n}
		}
	}

	pred := preds[0]
	next := pred.next[0]
	newMarkable := markable{next: newItem}
	old := (*markable)(atomic.SwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&pred.next[0])),
		unsafe.Pointer(&newMarkable),
	))
	if next != nil && old != next {
		return sk.cas(key, newVal) // retry
	}
	for level := 1; level < int(newItem.numLevels); level++ {
		for {
			curr := preds[level]
			if curr == nil {
				break
			}
			succ := curr.next[level]
			old = (*markable)(atomic.SwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&curr.next[level])),
				unsafe.Pointer(&newMarkable),
			))
			if old == succ {
				break
			}
			sk.findPreds(preds, nexts, int(newItem.numLevels), index, AssistUnlink)
			for i := level; i < int(newItem.numLevels); i++ {
				oldNext := newItem.next[i]
				succ := nexts[i]
				var next *markable
				if succ != nil {
					next = succ.next[i]
				}
				if next == oldNext {
					continue
				}
				old := atomic.SwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&newItem.next[i])),
					unsafe.Pointer(&next),
				)
				if hasMark((*markable)(unsafe.Pointer(old))) {
					sk.findPreds(nil, nil, 0, index, ForceUnlink)
					return nil, false
				}
			}
		}
	}
	if hasMark(newItem.next[newItem.numLevels-1]) {
		sk.findPreds(nil, nil, 0, index, ForceUnlink)
	}
	highWater := atomic.LoadUint32(&sk.highWater)
	if uint32(newItem.numLevels) > highWater {
		atomic.StoreUint32(&sk.highWater, uint32(newItem.numLevels))
	}
	return newVal, true
}

func (sk *SkipList) lookup(key []byte) ([]byte, bool) {
	item := sk.findPreds(nil, nil, 0, Hash(key), DontUnlink)
	if item != nil {
		return item.val, true
	}
	return nil, false
}

func (sk *SkipList) Set(k, v []byte) error {
	_, ok := sk.cas(k, v)
	if !ok {
		return errors.New("not able to set key/value")
	}
	return nil
}

func (sk *SkipList) Get(k []byte) ([]byte, bool) {
	return sk.lookup(k)
}

func (sk *SkipList) Count() uint64 {
	return 0
}

func (sk *SkipList) Height() uint64 {
	return 0
}

func (sk *SkipList) Print() {
	var line strings.Builder
	for level := MaxLevels - 1; level >= 0; level-- {
		item := sk.head
		if item.next[level] == nil {
			continue
		}
		line.WriteString(fmt.Sprintf("(%d)", level))
		for item != nil {
			curr := item.next[level]
			line.WriteString(fmt.Sprintf("%s", item))
			curr = stripMark(curr)
			item = getNode(curr)
		}
		line.WriteString("\n")
	}
	fmt.Println(line.String())
	line.Reset()
	item := sk.head
	for item != nil {
		line.WriteString(fmt.Sprintf("%s ", item))
		if item != sk.head {
			line.WriteString(fmt.Sprintf("[%d]", item.numLevels))
		} else {
			line.WriteString("[HEAD]")
		}
		for level := 0; level < int(item.numLevels); level++ {
			curr := stripMark(item.next[level])
			line.WriteString(fmt.Sprintf(" %s", getNode(curr)))
			if item == sk.head && item.next[level] == nil {
				break
			}
		}
		line.WriteString("\n")
		curr := stripMark(item.next[0])
		item = getNode(curr)
	}
	fmt.Println(line.String())
}

func (sk *SkipList) Range(f func(k, v []byte) bool) {
	curr := sk.head.next[0]
	for curr != nil {
		item := getNode(curr)
		ok := f(item.key, item.val)
		curr = item.next[0]
		if !ok || curr == nil {
			break
		}
	}
}
