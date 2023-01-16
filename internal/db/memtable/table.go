package memtable

import (
	"errors"
	"sync/atomic"
	"unsafe"

	gstable "github.com/blong14/gache/internal/db/sstable"
)

var ErrAllowedBytesExceeded = errors.New("memtable allowed bytes exceeded")

type MemTable struct {
	readBuffer *SkipList
	bytes      uint64
}

func New() *MemTable {
	return &MemTable{
		readBuffer: NewSkipList(),
	}
}

func (m *MemTable) buffer() *SkipList {
	reader := (*SkipList)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&m.readBuffer))))
	return reader
}

func (m *MemTable) Get(k []byte) ([]byte, bool) {
	return m.buffer().Get(k)
}

func (m *MemTable) Count() uint64 {
	return m.buffer().Count()
}

func (m *MemTable) Set(k, v []byte) error {
	err := m.buffer().Set(k, v)
	if err != nil {
		return err
	}
	byts := atomic.AddUint64(&m.bytes, uint64(len(k)+len(v)))
	if byts >= 4096*4096 {
		return ErrAllowedBytesExceeded
	}
	return nil
}

func (m *MemTable) Scan(k, v []byte, f func(k, v []byte) bool) {
	m.buffer().Scan(k, v, f)
}

func (m *MemTable) Range(f func(k, v []byte) bool) {
	m.buffer().Range(f)
}

func (m *MemTable) Flush(sstable *gstable.SSTable) error {
	reader := m.buffer()
	nReader := NewSkipList()
	for {
		if atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&m.readBuffer)),
			unsafe.Pointer(reader),
			unsafe.Pointer(nReader),
		) {
			atomic.StoreUint64(&m.bytes, 0)
			reader.Range(func(k, v []byte) bool {
				err := sstable.Set(k, v)
				return err == nil
			})
			return nil
		}
		reader = m.buffer()
	}
}
