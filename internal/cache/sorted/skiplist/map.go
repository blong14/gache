package skiplist

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const MaxHeight int = 16

type mapEntry struct {
	sync.RWMutex
	topLayer    int
	marked      bool
	fullyLinked bool
	nexts       []*mapEntry
	key         any
	value       any
}

func newMapEntry[K, V any](k K, v V) *mapEntry {
	return &mapEntry{
		key:   k,
		value: v,
		nexts: make([]*mapEntry, MaxHeight, MaxHeight),
	}
}

type SkipList[K any, V any] struct {
	sentinal   *mapEntry
	maxHeight  int
	comparator func(k, v K) int
	h          int
	count      int
}

func New[K any, V any](comp func(k, v K) int) *SkipList[K, V] {
	return &SkipList[K, V]{
		comparator: comp,
		sentinal: &mapEntry{
			nexts:    make([]*mapEntry, MaxHeight, MaxHeight),
			topLayer: MaxHeight,
		},
		maxHeight: MaxHeight,
	}
}

func (sl *SkipList[K, V]) skipSearch(key K, preds []*mapEntry, succs []*mapEntry) int {
	lFound := -1
	pred := sl.sentinal
	var curr *mapEntry
	for layer := sl.maxHeight - 1; layer >= 0; layer-- {
		curr = pred.nexts[layer]
		for curr != nil && sl.comparator(key, curr.key.(K)) > 0 {
			pred = curr
			curr = pred.nexts[layer]
		}
		if lFound == -1 && curr != nil && (sl.comparator(key, curr.key.(K)) == 0) {
			lFound = layer
		}
		preds[layer] = pred
		succs[layer] = curr
	}
	return lFound
}

func (sl *SkipList[K, V]) Print() {
	out := ""
	curr := sl.sentinal
	for curr != nil {
		for i := 0; i < sl.maxHeight; i++ {
			n := curr.nexts[i]
			if n != nil {
				out = out + fmt.Sprintf("\t(%s, %s)", n.key, n.value)
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
	preds := make([]*mapEntry, sl.maxHeight, sl.maxHeight)
	succs := make([]*mapEntry, sl.maxHeight, sl.maxHeight)
	lFound := sl.skipSearch(key, preds, succs)
	if lFound != -1 && succs[lFound].fullyLinked && !succs[lFound].marked {
		return succs[lFound].value.(V), true
	}
	return *new(V), false
}

func (sl *SkipList[K, V]) Count() int {
	return sl.count
}

func (sl *SkipList[K, V]) randomHeight() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(sl.maxHeight)
}

func (sl *SkipList[K, V]) Set(key K, value V) {
	var (
		topLayer = sl.randomHeight()
		preds    = make([]*mapEntry, sl.maxHeight, sl.maxHeight)
		succs    = make([]*mapEntry, sl.maxHeight, sl.maxHeight)
	)
	unlock := func() {
	loop:
		for _, m := range preds {
			if m == nil {
				continue loop
			}
			for m.TryLock() {
				m.Unlock()
				continue loop
			}
			m.Unlock()
		}
	}
	for {
		unlock()
		lFound := sl.skipSearch(key, preds, succs)
		if lFound != -1 {
			nodeFound := succs[lFound]
			if !nodeFound.marked {
				// block until fully linked
				for !nodeFound.fullyLinked {
				}
				// item already in the list return early
				return
			}
			continue
		}
		var (
			valid    = true
			pred     *mapEntry
			succ     *mapEntry
			prevPred *mapEntry
		)
		for layer := 0; valid && (layer <= topLayer); layer++ {
			pred = preds[layer]
			succ = succs[layer]
			if pred != prevPred {
				for !pred.TryLock() {
					continue
				}
				prevPred = pred
			}
			if pred != nil && succ != nil {
				valid = !pred.marked && !succ.marked && pred.nexts[layer] == succ
			}
		}
		if !valid {
			// validation failed; try again
			// validation = for each layer, i <= topNodeLayer, preds[i], succs[i]
			// are still adjacent at layer i and that neither is marked
			continue
		}
		// at this point; this thread holds all locks
		// safe to create a new node
		newNode := newMapEntry(key, value)
		newNode.topLayer = topLayer
		for layer := 0; layer <= topLayer; layer++ {
			newNode.nexts[layer] = succs[layer]
			preds[layer].nexts[layer] = newNode
		}
		newNode.fullyLinked = true
		sl.count++
		unlock()
		return
	}
}
