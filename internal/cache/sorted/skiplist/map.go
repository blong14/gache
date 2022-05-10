package skiplist

type mapEntry struct {
	top    *mapEntry
	right  *mapEntry
	bottom *mapEntry
	left   *mapEntry

	key   any
	value any
}

var sentinal *mapEntry = nil

type SkipList[K any, V any] struct {
	head       *mapEntry
	comparator func(k, v any) int
	h          int
}

func New[K any, V any](comp func(k, v any) int) *SkipList[K, V] {
	return &SkipList[K, V]{
		comparator: comp,
	}
}

func (sl *SkipList[K, V]) Get(key K) (V, bool) {
	p := sl.head
	for p.bottom != sentinal {
		p = p.bottom
		for sl.comparator(key, p.right.key.(K)) >= 0 {
			p = p.right
		}
	}
	if p == sentinal {
		return *new(V), false
	}
	return p.value.(V), true
}
