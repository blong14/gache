package skiplist

import (
	"fmt"
	"hash/maphash"
	"sync/atomic"
	_ "unsafe"
)

// XUint32n returns a lock free uint32 value in the interval [0, n).
//
//go:linkname XUint32n runtime.fastrandn
func XUint32n(n uint32) uint32

const MaxHeight uint8 = 20

var seed = maphash.MakeSeed()

func Hash(key []byte) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	_, _ = h.Write(key)
	return h.Sum64()
}

func unlock(highestLocked int, preds []*MapEntry) {
	if highestLocked < 0 {
		return
	}
	for _, m := range preds {
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

type MapEntry struct {
	key         uint64
	topLayer    uint8
	marked      bool
	fullyLinked bool
	nexts       [MaxHeight]*MapEntry
	lock        chan struct{}

	value  []byte
	rawKey []byte
}

func NewMapEntry(k uint64, v []byte) *MapEntry {
	return &MapEntry{
		lock:  make(chan struct{}, 1),
		key:   k,
		value: v,
		nexts: [MaxHeight]*MapEntry{},
	}
}

type SkipList struct {
	Sentinal  *MapEntry
	MaxHeight uint8
	H         uint64
	count     uint64
}

func New() *SkipList {
	return &SkipList{
		Sentinal:  NewMapEntry(0, []byte("")),
		MaxHeight: MaxHeight,
		H:         uint64(0),
		count:     uint64(0),
	}
}

func (sl *SkipList) skipSearch(key uint64, preds, succs []*MapEntry) int {
	var curr *MapEntry
	pred := sl.Sentinal
	layer := int(sl.MaxHeight - 1)
oloop:
	curr = pred.nexts[layer]
iloop:
	if curr != nil && key > curr.key {
		pred = curr
		curr = pred.nexts[layer]
		goto iloop
	}
	preds[layer] = pred
	succs[layer] = curr
	if curr != nil && key == curr.key {
		return layer
	}
	layer--
	if layer >= 0 {
		goto oloop
	}
	return -1
}

func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	preds := make([]*MapEntry, MaxHeight)
	succs := make([]*MapEntry, MaxHeight)
	lFound := sl.skipSearch(Hash(key), preds, succs)
	if lFound != -1 && succs[lFound].fullyLinked && !succs[lFound].marked {
		return succs[lFound].value, true
	}
	return nil, false
}

func (sl *SkipList) Set(key, value []byte) error {
	var (
		valid    bool
		topLayer = XUint32n(uint32(MaxHeight))
		pred     *MapEntry
		succ     *MapEntry
		prevPred *MapEntry
	)
	if topLayer == 0 {
		topLayer = 1
	}
loop:
	for {
		valid = true
		highestLocked := -1
		locks := make([]*MapEntry, MaxHeight)
		preds := make([]*MapEntry, MaxHeight)
		succs := make([]*MapEntry, MaxHeight)
		lFound := sl.skipSearch(Hash(key), preds, succs)
		if lFound != -1 {
			nodeFound := succs[lFound]
			if nodeFound != nil && !nodeFound.marked {
				// item already in the list return early
				unlock(highestLocked, locks)
				return nil
			}
			continue
		}
		height := sl.Height()
		for layer := uint64(0); valid && (layer <= height); layer++ {
			pred = preds[layer]
			succ = succs[layer]
			if pred != prevPred {
				select {
				case pred.lock <- struct{}{}:
					prevPred = pred
					highestLocked = int(layer)
					locks[layer] = pred
				default:
					unlock(highestLocked, locks)
					continue loop
				}
			}
			if succ != nil {
				valid = !pred.marked && !succ.marked && pred.nexts[layer] == succ
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
		newNode := NewMapEntry(Hash(key), value)
		newNode.topLayer = uint8(topLayer)
		newNode.rawKey = key
		for layer := uint64(0); layer <= uint64(topLayer); layer++ {
			newNode.nexts[layer] = succs[layer]
			preds[layer].nexts[layer] = newNode
		}
		newNode.fullyLinked = true
		count := atomic.AddUint64(&sl.count, 1)
		atomic.StoreUint64(&sl.count, count)
		height = sl.Height()
		if uint64(topLayer) > height {
			atomic.StoreUint64(&sl.H, uint64(topLayer))
		}
		unlock(highestLocked, locks)
		return nil
	}
}

func (sl *SkipList) Remove(k uint64) ([]byte, bool) {
	return nil, true
}

func (sl *SkipList) Print() {
	out := ""
	curr := sl.Sentinal
	for curr != nil {
		for i := uint8(0); i < sl.MaxHeight; i++ {
			n := curr.nexts[i]
			if n != nil {
				out = out + fmt.Sprintf("\t(%v, %d)", n.key, i)
			}
		}
		curr = curr.nexts[0]
		out = out + "\n"
	}
	fmt.Println(out)
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
	return atomic.LoadUint64(&sl.H)
}
