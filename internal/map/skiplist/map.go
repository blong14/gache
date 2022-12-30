package skiplist

import (
	"fmt"
	"hash/maphash"
	"strings"
	"sync/atomic"
	"unsafe"
	_ "unsafe"
)

var seed = maphash.MakeSeed()

func hash(key []byte) uint64 {
	var hasher maphash.Hash
	hasher.SetSeed(seed)
	_, _ = hasher.Write(key)
	return hasher.Sum64()
}

// XUint32n returns a lock free uint32 value in the interval [0, n).
//
//go:linkname XUint32n runtime.fastrandn
func XUint32n(n uint32) uint32

const maxHeight uint8 = 20

func unlock(highestLocked int, preds []*node) {
	if highestLocked < 0 || highestLocked >= len(preds) {
		return
	}
	for i := 0; i <= highestLocked; i++ {
		m := preds[i]
		if m == nil {
			continue
		}
		if m.lock != nil {
			select {
			case <-m.lock:
			default:
			}
		}
	}
}

type node struct {
	rawKey      []byte
	value       []byte
	hash        uint64
	topLayer    uint8
	marked      bool
	fullyLinked bool
	nexts       [maxHeight]*node
	lock        chan struct{}
}

func newNode(k, v []byte) *node {
	return &node{
		lock:   make(chan struct{}, 1),
		rawKey: k,
		value:  v,
		nexts:  [maxHeight]*node{},
	}
}

func (n *node) Next(layer uint64) *node {
	return (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&n.nexts[layer]))))
}

type SkipList struct {
	Sentinal  *node
	MaxHeight uint8
	height    uint64
	count     uint64
}

func New() *SkipList {
	return &SkipList{
		Sentinal:  newNode(nil, nil),
		MaxHeight: maxHeight,
		height:    uint64(0),
		count:     uint64(0),
	}
}

func (sl *SkipList) search(key uint64, preds, succs []*node) int {
	var curr *node
	pred := sl.Sentinal
	layer := int(sl.MaxHeight - 1)
oloop:
	curr = pred.Next(uint64(layer))
iloop:
	// d := -1
	if curr != nil {
		// d = bytes.Compare(key, curr.rawKey)
		if key > curr.hash {
			pred = curr
			curr = pred.Next(uint64(layer))
			goto iloop
		}
	}
	preds[layer] = pred
	succs[layer] = curr
	if curr != nil && key == curr.hash {
		return layer
	}
	layer--
	if layer >= 0 {
		goto oloop
	}
	return -1
}

func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	preds := make([]*node, maxHeight)
	succs := make([]*node, maxHeight)
	k := hash(key)
	lFound := sl.search(k, preds, succs)
	if lFound != -1 && succs[lFound].fullyLinked && !succs[lFound].marked {
		return succs[lFound].value, true
	}
	return nil, false
}

func (sl *SkipList) Set(key, value []byte) error {
	var (
		valid    bool
		pred     *node
		succ     *node
		prevPred *node
	)
	topLayer := XUint32n(uint32(maxHeight))
	if topLayer == 0 {
		topLayer = 1
	}
	k := hash(key)
loop:
	for {
		valid = true
		highestLocked := -1
		preds := make([]*node, maxHeight)
		succs := make([]*node, maxHeight)
		locks := make([]*node, maxHeight)
		lFound := sl.search(k, preds, succs)
		if lFound != -1 {
			nodeFound := succs[lFound]
			if nodeFound != nil && !nodeFound.marked {
				// item already in the list return early
				return nil
			}
			continue
		}
		height := sl.Height()
		for layer := uint64(0); valid && (layer <= height); layer++ {
			pred = preds[layer]
			succ = succs[layer]
			if pred != nil && pred != prevPred {
				select {
				case pred.lock <- struct{}{}:
					locks[layer] = pred
					highestLocked = int(layer)
					prevPred = pred
				default:
					unlock(highestLocked, locks)
					continue loop
				}
			}
			if succ != nil {
				valid = !pred.marked && !succ.marked && pred.Next(layer) == succ
			}
		}
		if !valid {
			// validation failed; try again
			// validation = for each layer, i <= topNodeLayer, preds[i], succs[i]
			// are still adjacent at layer i and that neither is marked
			unlock(highestLocked, locks)
			continue
		}
		// at this point; this thread holds all locks
		// safe to create a new node
		node := &node{}
		node.lock = make(chan struct{}, 1)
		node.rawKey = key
		node.hash = k
		node.value = value
		node.topLayer = uint8(topLayer)
		for layer := uint64(0); layer <= uint64(topLayer); layer++ {
			node.nexts[layer] = succs[layer]
			pred := preds[layer]
			n := pred.Next(layer)
			atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&pred.nexts[layer])),
				unsafe.Pointer(n),
				unsafe.Pointer(node),
			)
		}
		node.fullyLinked = true
		atomic.AddUint64(&sl.count, 1)
		height = sl.Height()
		for uint64(topLayer) > height {
			if atomic.CompareAndSwapUint64(&sl.height, height, uint64(topLayer)) {
				break
			}
			height = sl.Height()
		}
		unlock(highestLocked, locks)
		return nil
	}
}

func (sl *SkipList) Remove(k uint64) ([]byte, bool) {
	return nil, true
}

func (sl *SkipList) Print() {
	out := strings.Builder{}
	out.Reset()
	curr := sl.Sentinal
	for curr != nil {
		for i := uint8(0); i < sl.MaxHeight; i++ {
			n := curr.nexts[i]
			if n != nil {
				out.WriteString(fmt.Sprintf("\t(%d, %s)", n.hash, n.rawKey))
			}
		}
		curr = curr.nexts[0]
		out.WriteString("\n")
	}
	fmt.Println(out.String())
}

func (sl *SkipList) Range(f func(k, v []byte) bool) {
	curr := sl.Sentinal.nexts[0]
	for curr != nil {
		ok := f(curr.rawKey, curr.value)
		curr = curr.nexts[0]
		if !ok || curr == nil {
			break
		}
	}
}

func (sl *SkipList) Count() uint64 {
	return atomic.LoadUint64(&sl.count)
}

func (sl *SkipList) Height() uint64 {
	return atomic.LoadUint64(&sl.height)
}
