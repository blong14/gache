package skiplist

import (
	"fmt"
	"hash/maphash"
	"sync/atomic"
	"time"
)

const MaxHeight uint8 = 20

type PRNG struct {
	seed uint64
}

func (p *PRNG) Seed(seed uint64) {
	p.seed = seed
}

func (p *PRNG) Next() uint64 {
	p.seed = p.seed + 1
	a := p.seed * 15485863
	// Will return in range 0 to 1 if seed >= 0 and -1 to 0 if seed < 0.
	b := float64((a*a*a)%2038074743) / float64(2038074743)
	c := float64(MaxHeight) * b
	return uint64(c)
}

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
	var p PRNG
	p.Seed(uint64(time.Now().UnixNano()))
	var (
		valid    bool
		topLayer = p.Next()
		pred     *MapEntry
		succ     *MapEntry
		prevPred *MapEntry
	)
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
		for layer := uint64(0); layer <= topLayer; layer++ {
			newNode.nexts[layer] = succs[layer]
			preds[layer].nexts[layer] = newNode
		}
		newNode.fullyLinked = true
		count := atomic.AddUint64(&sl.count, 1)
		atomic.StoreUint64(&sl.count, count)
		height = sl.Height()
		if topLayer > height {
			atomic.StoreUint64(&sl.H, topLayer)
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
