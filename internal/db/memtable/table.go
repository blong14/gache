package memtable

import (
	"bytes"
	"errors"
	"sync/atomic"
	"unsafe"

	gstable "github.com/blong14/gache/internal/db/sstable"
	gskl "github.com/blong14/gache/internal/map/xskiplist"
)

var ErrAllowedBytesExceeded = errors.New("memtable allowed bytes exceeded")

type MemTable struct {
	// flush      chan struct{}
	readBuffer *gskl.SkipList
	bytes      uint64
}

func New() *MemTable {
	return &MemTable{
		readBuffer: gskl.New(),
	}
}

func (m *MemTable) buffer() *gskl.SkipList {
	reader := (*gskl.SkipList)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&m.readBuffer))))
	return reader
}

func (m *MemTable) Get(k []byte) ([]byte, bool) {
	return m.buffer().Get(k)
}

func (m *MemTable) Scan(start, end []byte) ([][][]byte, bool) {
	out := make([][][]byte, 0)
	return out, true
}

func (m *MemTable) Set(k, v []byte) error {
	err := m.buffer().Set(k, v)
	if err != nil {
		return err
	}
	byts := atomic.AddUint64(&m.bytes, uint64(len(k)+len(v)))
	if byts >= 4096 {
		return ErrAllowedBytesExceeded
	}
	return nil
}

func (m *MemTable) Flush(sstable *gstable.SSTable) error {
	reader := m.buffer()
	nReader := gskl.New()
	for {
		if atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&m.readBuffer)),
			unsafe.Pointer(reader),
			unsafe.Pointer(nReader),
		) {
			atomic.StoreUint64(&m.bytes, 0)
			buf := bytes.NewBuffer(nil)
			buf.Reset()
			reader.Range(func(k, v []byte) bool {
				buf.Write(k)
				buf.Write([]byte("::"))
				buf.Write(v)
				buf.Write([]byte("\n"))
				return true
			})
			err := sstable.XSet(buf)
			if err != nil {
				return err
			}
			return nil
		}
		reader = m.buffer()
	}
}
