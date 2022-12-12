package memtable

import (
	"sync/atomic"
	"unsafe"

	gstable "github.com/blong14/gache/internal/db/sstable"
	gskl "github.com/blong14/gache/internal/map/skiplist"
)

type MemTable struct {
	readBuffer *gskl.SkipList
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

func (m *MemTable) Set(k, v []byte) error {
	return m.buffer().Set(k, v)
}

func (m *MemTable) Flush(sstable *gstable.SSTable) error {
	reader := m.buffer()
	if reader.Count() > 4096 {
		nReader := gskl.New()
		for {
			if atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&m.readBuffer)),
				unsafe.Pointer(reader),
				unsafe.Pointer(nReader),
			) {
				reader.Range(func(k, v []byte) bool {
					err := sstable.Set(k, v)
					return err == nil
				})
				return nil
			}
			reader = m.buffer()
		}
	}
	return nil
}
