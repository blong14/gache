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
	key         any
	value       any
}

func NewMapEntry[K, V any](k K, v V) *mapEntry {
	return &mapEntry{
		lock:  make(chan struct{}, 1),
		key:   k,
		value: v,
		nexts: [MaxHeight]*mapEntry{},
	}
}

type SkipList[K any, V any] struct {
	Sentinal   *mapEntry
	MaxHeight  uint8
	Comparator func(k, v K) int
	H          uint64
	count      uint64
}

func New[K any, V any](comp func(k K, v K) int) *SkipList[K, V] {
	return &SkipList[K, V]{
		Comparator: comp,
		Sentinal:   NewMapEntry[K, V](*new(K), *new(V)),
		MaxHeight:  MaxHeight,
		H:          uint64(0),
		count:      uint64(0),
	}
}

func (sl *SkipList[K, V]) skipSearch(key K, preds, succs []*mapEntry) int {
	pred := sl.Sentinal
	var curr *mapEntry
	for layer := int(sl.MaxHeight - 1); layer >= 0; layer-- {
		curr = pred.nexts[layer]
		for curr != nil && sl.Comparator(key, curr.key.(K)) > 0 {
			pred = curr
			curr = pred.nexts[layer]
		}
		preds[layer] = pred
		succs[layer] = curr
		if curr != nil && sl.Comparator(key, curr.key.(K)) == 0 {
			return layer
		}
	}
	return -1
}

func (sl *SkipList[K, V]) Remove(k K) (V, bool) {
	return *new(V), true
}

func (sl *SkipList[K, V]) Print() {
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

func (sl *SkipList[K, V]) Range(f func(k K, v V) bool) {
	layer := 0
	curr := sl.Sentinal.nexts[layer]
	for curr != nil {
		if ok := f(curr.key.(K), curr.value.(V)); ok {
			curr = curr.nexts[layer]
		} else {
			return
		}
	}
}

func (sl *SkipList[K, V]) Get(key K) (V, bool) {
	preds := make([]*mapEntry, MaxHeight, MaxHeight)
	succs := make([]*mapEntry, MaxHeight, MaxHeight)
	lFound := sl.skipSearch(key, preds, succs)
	if lFound != -1 && succs[lFound].fullyLinked && !succs[lFound].marked {
		return succs[lFound].value.(V), true
	}
	return *new(V), false
}

func (sl *SkipList[K, V]) Count() uint64 {
	count := atomic.LoadUint64(&sl.count)
	return count
}

func (sl *SkipList[K, V]) randomHeight() uint64 {
	var p PRNG
	p.Seed(uint64(time.Now().UnixNano()))
	return p.Next()
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

func (sl *SkipList[K, V]) Set(key K, value V) {
	var (
		preds         = make([]*mapEntry, MaxHeight, MaxHeight)
		succs         = make([]*mapEntry, MaxHeight, MaxHeight)
		valid         bool
		topLayer      = sl.randomHeight()
		pred          *mapEntry
		succ          *mapEntry
		prevPred      *mapEntry
		highestLocked = -1
	)
	for {
		valid = true
		lFound := sl.skipSearch(key, preds, succs)
		if lFound != -1 {
			nodeFound := succs[lFound]
			if nodeFound != nil && !nodeFound.marked {
				// block until fully linked
				for !nodeFound.fullyLinked {
				}
				// item already in the list return early
				return
			}
			continue
		}
		height := atomic.LoadUint64(&sl.H)
		for layer := uint64(0); valid && (layer <= height); layer++ {
			pred = preds[layer]
			succ = succs[layer]
			if pred != prevPred {
				pred.lock <- struct{}{}
				highestLocked = int(layer)
				prevPred = pred
			}
			if succ != nil {
				valid = !pred.marked && !succ.marked && pred.nexts[layer] == succ
			}
		}
		if !valid {
			// validation failed; try again
			// validation = for each layer, i <= topNodeLayer, preds[i], succs[i]
			// are still adjacent at layer i and that neither is marked
			unlock(highestLocked, preds)
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
		unlock(highestLocked, preds)
		return
	}
}
