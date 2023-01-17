package arena

import "sync"

var mtx sync.Mutex

var ballast = 4096 * 4096

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
