package db

import (
	"hash/maphash"
	"os"

	garena "github.com/blong14/gache/internal/db/arena"
	gskl "github.com/blong14/gache/internal/db/sorted/skiplist"
)

var seed = maphash.MakeSeed()

func Hash(key []byte) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	_, _ = h.Write(key)
	return h.Sum64()
}

type Table[K, V any] interface {
	Get(k K) (V, bool)
	Print()
	Range(func(k K, v V) bool)
	Remove(k K) (V, bool)
	Set(k K, v V)
}

type TableOpts struct {
	TableName []byte
}

type TableCache struct {
	name     string
	index    *gskl.SkipList
	memtable garena.Arena
	handle   *os.File
}

func New() *TableCache {
	return &TableCache{
		index: gskl.New(),
	}
}

func XNew(name string) *TableCache {
	return &TableCache{
		name:  name,
		index: gskl.New(),
	}
}

func (db *TableCache) Open() {
	length := 4096 * 4096 * 4
	f, err := os.OpenFile(db.name, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}

	s, err := f.Stat()
	if err != nil {
		panic(err)
	}
	size := s.Size()
	if size == 0 {
		_, err = f.Write(make([]byte, length))
		if err != nil {
			panic(err)
		}
	}

	db.memtable = garena.NewGoArena(f, uint64(length))
	db.handle = f
}

func (db *TableCache) Get(k []byte) ([]byte, bool) {
	return db.index.Get(Hash(k))
}

func (db *TableCache) Print() {
	db.index.Print()
}

func (db *TableCache) Range(f func(k uint64, v []byte) bool) {
	db.index.Range(f)
}

func (db *TableCache) Remove(k []byte) ([]byte, bool) {
	return db.index.Remove(Hash(k))
}

func (db *TableCache) Set(k, v []byte) {
	db.index.Set(Hash(k), v)
}

func (db *TableCache) XSet(k, v []byte) {
	if _, err := db.memtable.XWrite(k, v); err != nil {
		db.index.Set(Hash(k), v)
	}
}

func (db *TableCache) Close() {
	if db.memtable != nil {
		db.memtable.Free()
	}
}
