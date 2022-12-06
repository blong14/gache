package memtable

import (
	gstable "github.com/blong14/gache/internal/db/sstable"
	gskl "github.com/blong14/gache/internal/map/skiplist"
	"sync/atomic"
	"unsafe"
)

type MemTable struct {
	readBuffer *gskl.SkipList
}

func New() *MemTable {
	return &MemTable{
		readBuffer: gskl.New(),
	}
}

func (m *MemTable) Get(k []byte) ([]byte, bool) {
	reader := (*gskl.SkipList)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&m.readBuffer))))
	return reader.Get(k)
}

func (m *MemTable) Set(k, v []byte) error {
	writer := (*gskl.SkipList)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&m.readBuffer))))
	return writer.Set(k, v)
}

func (m *MemTable) Flush(sstable *gstable.SSTable) error {
	nReader := gskl.New()
	for {
		reader := (*gskl.SkipList)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&m.readBuffer))))
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
	}
}
