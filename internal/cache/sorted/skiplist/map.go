package skiplist

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const MaxHeight uint8 = 20

type mapEntry struct {
	lock        chan struct{}
	topLayer    uint8
	marked      bool
	fullyLinked bool
	nexts       [MaxHeight]*mapEntry
	key         any
	value       any
}

func newMapEntry[K, V any](k K, v V) *mapEntry {
	return &mapEntry{
		lock:  make(chan struct{}, 1),
		key:   k,
		value: v,
		nexts: [MaxHeight]*mapEntry{},
	}
}

var pool = sync.Pool{
	New: func() any {
		return make([]*mapEntry, MaxHeight, MaxHeight)
	},
}

func free(preds, succs []*mapEntry) {
	pool.Put(preds)
	pool.Put(succs)
}

type SkipList[K any, V any] struct {
	sentinal   *mapEntry
	maxHeight  uint8
	comparator func(k, v K) int
	matcher    func(k, v K) bool
	h          uint64
	count      uint64
}

func New[K any, V any](comp func(k, v K) int) *SkipList[K, V] {
	return &SkipList[K, V]{
		comparator: comp,
		sentinal: &mapEntry{
			lock:     make(chan struct{}, 1),
			nexts:    [MaxHeight]*mapEntry{},
			topLayer: MaxHeight,
		},
		maxHeight: MaxHeight,
	}
}

func XNew[K any, V any](comp func(k, v K) int, eql func(k, v K) bool) *SkipList[K, V] {
	if eql == nil {
		eql = func(k, v K) bool { return comp(k, v) == 0 }
	}
	return &SkipList[K, V]{
		comparator: comp,
		matcher:    eql,
		sentinal: &mapEntry{
			lock:     make(chan struct{}, 1),
			nexts:    [MaxHeight]*mapEntry{},
			topLayer: MaxHeight,
		},
		maxHeight: MaxHeight,
		h:         uint64(0),
		count:     uint64(0),
	}
}

func (sl *SkipList[K, V]) skipSearch(key K, preds, succs []*mapEntry) int {
	lFound := -1
	pred := sl.sentinal
	var curr *mapEntry
	for layer := int(sl.maxHeight - 1); layer >= 0; layer-- {
		curr = pred.nexts[layer]
		for curr != nil && sl.comparator(key, curr.key.(K)) > 0 {
			pred = curr
			curr = pred.nexts[layer]
		}
		preds[layer] = pred
		succs[layer] = curr
		if curr != nil && sl.matcher(key, curr.key.(K)) {
			lFound = layer
			return lFound
		}
	}
	return lFound
}

func (sl *SkipList[K, V]) Print() {
	out := ""
	curr := sl.sentinal
	for curr != nil {
		for i := uint8(0); i < sl.maxHeight; i++ {
			n := curr.nexts[i]
			if n != nil {
				out = out + fmt.Sprintf("\t(%s, %d)", n.key, i)
			}
		}
		curr = curr.nexts[0]
		out = out + "\n"
	}
	fmt.Println(out)
}

func (sl *SkipList[K, V]) Range(f func(k, v any) bool) {
	layer := 0
	curr := sl.sentinal.nexts[layer]
	for curr != nil {
		if ok := f(curr.key, curr.value); ok {
			curr = curr.nexts[layer]
		} else {
			return
		}
	}
}

func (sl *SkipList[K, V]) Get(key K) (V, bool) {
	var (
		preds = pool.Get().([]*mapEntry)
		succs = pool.Get().([]*mapEntry)
	)
	defer free(preds, succs)
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
	// https://www.jstatsoft.org/article/download/v008i14/916
	idx := time.Now().UnixNano()
	x := idx << 13 // magic prime numbers
	x = x >> 7
	x = x << 17
	return uint64(x) % uint64(sl.maxHeight)
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
		preds = pool.Get().([]*mapEntry)
		succs = pool.Get().([]*mapEntry)
	)
	defer free(preds, succs)
	var (
		topLayer      = sl.randomHeight()
		valid         = true
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

		height := atomic.LoadUint64(&sl.h)
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
		newNode := newMapEntry(key, value)
		newNode.topLayer = uint8(topLayer)
		for layer := uint64(0); layer <= topLayer; layer++ {
			newNode.nexts[layer] = succs[layer]
			preds[layer].nexts[layer] = newNode
		}
		newNode.fullyLinked = true
		atomic.AddUint64(&sl.count, 1)
		height = atomic.LoadUint64(&sl.h)
		if topLayer > height {
			atomic.StoreUint64(&sl.h, topLayer)
		}
		unlock(highestLocked, preds)
		return
	}
}
