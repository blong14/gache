package arena

import (
	"sync"

	ga "arena"
)

var mtx sync.Mutex

var ballast = 4096

type ByteArena []byte

func (na *ByteArena) Allocate(len_ int) []byte {
	mtx.Lock()
	defer mtx.Unlock()
	if len(*na) == 0 {
		*na = make([]byte, ballast)
		ballast *= 2
	}
	offset := (len(*na) - 1) - len_
	if offset <= 0 {
		*na = make([]byte, len(*na)+len_)
		offset = (len(*na) - 1) - len_
	}
	n := (*na)[offset : len(*na)-1]
	*na = (*na)[:offset]
	return n
}

type Arena interface {
	Allocator() *ga.Arena
	AllocateByteSlice(len_, cap int) []byte
	Free()
}

type arena struct {
	malloc *ga.Arena
}

func NewArena() Arena {
	return &arena{malloc: ga.NewArena()}
}

func (a *arena) Allocator() *ga.Arena {
	return a.malloc
}

func (a *arena) Free() {
	a.malloc.Free()
}

func (a *arena) AllocateByteSlice(len_, cap int) []byte {
	return ga.MakeSlice[byte](a.malloc, len_, cap)
}

func MakeByteSlice(alloc *ga.Arena, len_, cap int) []byte {
	return ga.MakeSlice[byte](alloc, len_, cap)
}

func MakeNew[T any](alloc *ga.Arena) *T {
	return ga.New[T](alloc)
}
