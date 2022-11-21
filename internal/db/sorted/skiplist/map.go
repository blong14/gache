package skiplist

import (
	"fmt"
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

type mapEntry struct {
	lock        chan struct{}
	topLayer    uint8
	marked      bool
	fullyLinked bool
	nexts       [MaxHeight]*mapEntry
	Key         uint64
	Value       []byte
}

func NewMapEntry(k uint64, v []byte) *mapEntry {
	return &mapEntry{
		lock:  make(chan struct{}, 1),
		Key:   k,
		Value: v,
		nexts: [MaxHeight]*mapEntry{},
	}
}

type SkipList struct {
	Sentinal  *mapEntry
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

func (sl *SkipList) skipSearch(key uint64, preds, succs []*mapEntry) int {
	var curr *mapEntry
	pred := sl.Sentinal
	layer := int(sl.MaxHeight - 1)
oloop:
	curr = pred.nexts[layer]
iloop:
	if curr != nil && key > curr.Key {
		pred = curr
		curr = pred.nexts[layer]
		goto iloop
	}
	preds[layer] = pred
	succs[layer] = curr
	if curr != nil && key == curr.Key {
		return layer
	}
	layer--
	if layer >= 0 {
		goto oloop
	}
	return -1
}

func (sl *SkipList) Get(key uint64) ([]byte, bool) {
	preds := make([]*mapEntry, MaxHeight)
	succs := make([]*mapEntry, MaxHeight)
	lFound := sl.skipSearch(key, preds, succs)
	if lFound != -1 && succs[lFound].fullyLinked && !succs[lFound].marked {
		return succs[lFound].Value, true
	}
	return nil, false
}

func unlock(highestLocked int, preds []*mapEntry) {
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

func (sl *SkipList) Set(key uint64, value []byte) {
	var p PRNG
	p.Seed(uint64(time.Now().UnixNano()))
	var (
		valid    bool
		topLayer = p.Next()
		pred     *mapEntry
		succ     *mapEntry
		prevPred *mapEntry
	)
loop:
	for {
		valid = true
		highestLocked := -1
		locks := make([]*mapEntry, MaxHeight)
		preds := make([]*mapEntry, MaxHeight)
		succs := make([]*mapEntry, MaxHeight)
		lFound := sl.skipSearch(key, preds, succs)
		if lFound != -1 {
			nodeFound := succs[lFound]
			if nodeFound != nil && !nodeFound.marked {
				// item already in the list return early
				unlock(highestLocked, locks)
				return
			}
			continue
		}
		height := atomic.LoadUint64(&sl.H)
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
		newNode := NewMapEntry(key, value)
		newNode.topLayer = uint8(topLayer)
		for layer := uint64(0); layer <= topLayer; layer++ {
			newNode.nexts[layer] = succs[layer]
			preds[layer].nexts[layer] = newNode
		}
		newNode.fullyLinked = true
		atomic.AddUint64(&sl.count, 1)
		height = atomic.LoadUint64(&sl.H)
		if topLayer > height {
			atomic.StoreUint64(&sl.H, topLayer)
		}
		unlock(highestLocked, locks)
		return
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
				out = out + fmt.Sprintf("\t(%v, %d)", n.Key, i)
			}
		}
		curr = curr.nexts[0]
		out = out + "\n"
	}
	fmt.Println(out)
}

func (sl *SkipList) Range(f func(k uint64, v []byte) bool) {
	curr := sl.Sentinal.nexts[0]
loop:
	ok := f(curr.Key, curr.Value)
	curr = curr.nexts[0]
	if ok && curr != nil {
		goto loop
	}
}
