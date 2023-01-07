package memtable

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	glog "github.com/blong14/gache/internal/logging"
)

// RandUint32 returns a lock free uint32 value.
//
//go:linkname RandUint32 runtime.fastrand
func RandUint32() uint32

func hash(key []byte) uint64 {
	var h uint64
	for _, b := range key {
		h = uint64(b) + (h << 6) + (h << 16) - h
	}
	return h
}

type node struct {
	hash uint64
	next *node
	key  []byte
	val  []byte
}

func newNode(h uint64, k, v []byte, n *node) *node {
	return &node{
		hash: h,
		key:  k,
		val:  v,
		next: n,
	}
}

func (n *node) Next() *node {
	if n == nil {
		return nil
	}
	return (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&n.next))))
}

var mtx sync.Mutex

var ballast = 2048

type NodeArena []*node

func (na *NodeArena) allocate() *node {
	mtx.Lock()
	defer mtx.Unlock()
	if len(*na) == 0 {
		tmp := make([]*node, ballast)
		for i := 0; i < ballast; i++ {
			tmp[i] = newNode(0, nil, nil, nil)
		}
		*na = tmp
		ballast = ballast * 2
	}
	n := &(*na)[len(*na)-1]
	*na = (*na)[:len(*na)-1]
	return *n
}

var nodeArena NodeArena

type index struct {
	node  *node
	down  *index
	right *index
}

func newIndex(n *node, down, right *index) *index {
	return &index{
		node:  n,
		down:  down,
		right: right,
	}
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

var imtx sync.Mutex

var iballast = 2048

type IndexArena []*index

func (na *IndexArena) allocate(nd *node, d *index, r *index) *index {
	imtx.Lock()
	defer imtx.Unlock()
	if len(*na) == 0 {
		tmp := make([]*index, iballast)
		for i := 0; i < iballast; i++ {
			tmp[i] = newIndex(nil, nil, nil)
		}
		*na = tmp
		iballast = iballast * 2
	}
	n := &(*na)[len(*na)-1]
	(*n).node = nd
	(*n).down = d
	(*n).right = r
	*na = (*na)[:len(*na)-1]
	return *n
}

var indexArena IndexArena

type SkipList struct {
	head  *index
	count uint64
}

func NewSkipList() *SkipList {
	return &SkipList{}
}

func (sk *SkipList) top() *index {
	if sk == nil {
		return nil
	}
	return (*index)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&sk.head))))
}

func (sk *SkipList) findPredecessor(key uint64) *node {
	q := sk.top()
	for q != nil {
		r := q.Right()
		for r != nil {
			p := r.Node()
			if p == nil || p.hash == 0 || p.val == nil {
				atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&q.right)),
					unsafe.Pointer(r),
					unsafe.Pointer(r.Right()),
				)
			} else if key > p.hash {
				q = r
				r = q.Right()
			} else {
				break
			}
		}
		d := q.Down()
		if d == nil {
			return q.Node()
		}
		q = d
	}
	return nil
}

func (sk *SkipList) findNode(key uint64) *node {
	r := sk.findPredecessor(key)
	for r != nil {
		n := r.Next()
		for n != nil {
			if key > n.hash {
				r = n
				n = r.Next()
			} else if key == n.hash {
				return n
			} else {
				return nil
			}
		}
	}
	return nil
}

func (sk *SkipList) addIndices(q *index, skips int, x *index) bool {
	if x != nil && q != nil {
		z := x.Node()
		key := z.hash
		if key == 0 {
			return false
		}
		var retrying bool
		for {
			c := -1
			r := q.Right()
			if r != nil {
				p := r.Node()
				if p == nil || p.hash == 0 || p.val == nil {
					atomic.CompareAndSwapPointer(
						(*unsafe.Pointer)(unsafe.Pointer(&q.right)),
						unsafe.Pointer(r),
						unsafe.Pointer(r.Right()),
					)
					c = 0
				} else if key > p.hash {
					q = r
					r = q.Right()
					c = 1
				} else if key == p.hash {
					c = 0
				}
				if c == 0 {
					break
				}
			} else {
				c = -1
			}
			if c < 0 {
				d := q.Down()
				if d != nil && skips > 0 {
					skips -= 1
					q = d
				} else if d != nil && !retrying && !sk.addIndices(d, 0, x.Down()) {
					break
				} else {
					x.right = r
					if atomic.CompareAndSwapPointer(
						(*unsafe.Pointer)(unsafe.Pointer(&q.right)),
						unsafe.Pointer(r),
						unsafe.Pointer(x),
					) {
						return true
					} else {
						retrying = true
					}
				}
			}
		}
	}
	return false
}

func (sk *SkipList) Get(key []byte) ([]byte, bool) {
	hashedValue := hash(key)
	if hashedValue == 0 {
		return nil, false
	}
	q := sk.top()
	for q != nil {
		r := q.Right()
	loop:
		for r != nil {
			p := r.Node()
			switch {
			case p == nil || p.hash == 0 || p.val == nil:
				atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&q.right)),
					unsafe.Pointer(r),
					unsafe.Pointer(r.Right()),
				)
			case hashedValue > p.hash:
				q = r
				r = q.Right()
			case hashedValue == p.hash:
				return p.val, true
			default:
				break loop
			}
		}
		d := q.Down()
		if d != nil {
			q = d
		} else {
			b := q.Node()
			if b != nil {
				n := b.Next()
				for n != nil {
					if n.val == nil || n.hash == 0 || hashedValue > n.hash {
						b = n
						n = b.Next()
					} else {
						if hashedValue == n.hash {
							return n.val, true
						}
						break
					}
				}
			}
			break
		}
	}
	return nil, false
}

func (sk *SkipList) Set(key, value []byte) error {
	if key == nil {
		return errors.New("missing key")
	}
	var b *node
	hashedKey := hash(key)
	for {
		levels := 0
		h := sk.top()
		if h == nil {
			base := nodeArena.allocate()
			nh := indexArena.allocate(base, nil, nil)
			if atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&sk.head)),
				unsafe.Pointer(h),
				unsafe.Pointer(nh),
			) {
				b = base
				h = nh
			} else {
				b = nil
			}
		} else {
			q := h
			for q != nil {
				r := q.Right()
				for r != nil {
					p := r.Node()
					if p == nil || p.hash == 0 || p.val == nil {
						atomic.CompareAndSwapPointer(
							(*unsafe.Pointer)(unsafe.Pointer(&q.right)),
							unsafe.Pointer(r),
							unsafe.Pointer(r.Right()),
						)
					} else if hashedKey > p.hash {
						q = r
						r = q.Right()
					} else {
						break
					}
				}
				if q != nil {
					d := q.Down()
					if d != nil {
						levels += 1
						q = d
					} else {
						b = q.Node()
						break
					}
				}
			}
		}
		if b != nil {
			var z *node
			for {
				c := -1
				n := b.Next()
				if n == nil {
					c = -1
				} else if n.hash == 0 {
					break
				} else if n.val == nil {
					// unlinkNode(b, n)
					c = 1
				} else if hashedKey > n.hash {
					b = n
					c = 1
				} else if hashedKey == n.hash {
					c = 0
				}
				if c == 0 {
					// already in list
					return nil
				}
				if c < 0 {
					p := nodeArena.allocate()
					p.hash = hashedKey
					p.key = key
					p.val = value
					p.next = n
					if atomic.CompareAndSwapPointer(
						(*unsafe.Pointer)(unsafe.Pointer(&b.next)),
						unsafe.Pointer(n),
						unsafe.Pointer(p),
					) {
						z = p
						break
					}
				}
			}
			if z != nil {
				lr := uint64(RandUint32())
				if (lr & 0x3) == 0 {
					hr := uint64(RandUint32())
					rnd := hr<<32 | lr&0xffffffff
					skips := levels
					var x *index
					for {
						skips -= 1
						x = indexArena.allocate(z, x, nil)
						if rnd <= 0 || skips < 0 {
							break
						} else {
							rnd >>= 1
						}
					}
					if sk.addIndices(h, skips, x) && skips < 0 && sk.top() == h {
						hx := indexArena.allocate(z, x, nil)
						nh := indexArena.allocate(h.Node(), h, hx)
						atomic.CompareAndSwapPointer(
							(*unsafe.Pointer)(unsafe.Pointer(&sk.head)),
							unsafe.Pointer(h),
							unsafe.Pointer(nh),
						)
					}
					if z.val == nil {
						sk.findPredecessor(hashedKey)
					}
				}
				atomic.AddUint64(&sk.count, 1)
				return nil
			}
		}
	}
}

func (sk *SkipList) Remove(_ uint64) ([]byte, bool) {
	return nil, true
}

func (sk *SkipList) Range(f func(k, v []byte) bool) {
	h := sk.top()
	if h == nil || h.Node() == nil {
		return
	}
	b := h.Node()
	if b != nil {
		n := b.Next()
		for n != nil {
			if n.val != nil {
				ok := f(n.key, n.val)
				if !ok {
					break
				}
			}
			b = n
			n = b.Next()
		}
	}
}

type iter struct {
	sk           *SkipList
	lastReturned *node
	nxt          *node
	start        *uint64
	end          *uint64
}

func newIter(sk *SkipList, start, end []byte) *iter {
	var s *uint64
	if start != nil {
		h := hash(start)
		s = &h
	}
	var e *uint64
	if end != nil {
		h := hash(end)
		e = &h
	}
	i := &iter{sk: sk, start: s, end: e}
	h := i.sk.top()
	if h != nil {
		n := h.Node()
		i.advance(n)
	}
	return i
}

func (i *iter) advance(b *node) {
	var n *node
	i.lastReturned = b
	if i.lastReturned != nil {
		for n = b.Next(); n != nil && n.val == nil; {
			b = n
			n = b.Next()
		}
	}
	if i.start != nil && n != nil && *i.start > n.hash {
		n = i.sk.findNode(*i.start)
	}
	i.nxt = n
}

func (i *iter) hasNext() bool {
	if i.end == nil {
		return i.nxt != nil
	}
	return i.nxt != nil && i.nxt.hash <= *i.end
}

func (i *iter) next() *node {
	n := i.nxt
	i.advance(n)
	return n
}

func (sk *SkipList) Scan(start, end []byte, f func(k, v []byte) bool) {
	itr := newIter(sk, start, end)
	for itr.hasNext() {
		n := itr.next()
		if ok := f(n.key, n.val); !ok {
			return
		}
	}
}

func (sk *SkipList) Print() {
	out := strings.Builder{}
	out.Reset()
	curr := sk.top()
	d := curr.Down()
	for curr != nil {
		r := curr.Right()
		for r != nil {
			n := r.Node()
			out.WriteString(fmt.Sprintf("[%d - %s->]\t", n.hash, n.key))
			curr = r
			r = curr.Right()
		}
		if d.Down() != nil {
			curr = d
			d = d.Down()
			out.WriteString("\n")
		} else {
			out.WriteString("\n")
			curr = d
			for curr != nil {
				n := curr.Node()
				for n != nil {
					if n.hash == curr.Node().hash {
						out.WriteString(fmt.Sprintf("[%d-%s->] ", n.hash, n.key))
					} else {
						out.WriteString(fmt.Sprintf("%s-> ", n.key))
					}
					n = n.Next()
				}
				curr = r
				if curr != nil {
					r = curr.Right()
				}
			}
			break
		}
	}
	fmt.Println(out.String())
}

func (sk *SkipList) Count() uint64 {
	return atomic.LoadUint64(&sk.count)
}

func init() {
	glog.Track("allocating memory")
	nodeArena = make(NodeArena, ballast)
	for i := 0; i < ballast; i++ {
		nodeArena[i] = newNode(0, nil, nil, nil)
	}
	indexArena = make(IndexArena, iballast)
	for i := 0; i < iballast; i++ {
		indexArena[i] = newIndex(nil, nil, nil)
	}
}