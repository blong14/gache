package cache

import (
	"hash/maphash"

	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
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
	impl Table[uint64, []byte]
}

func New() *TableCache {
	return &TableCache{
		impl: gskl.New[uint64, []byte](Uint64Compare),
	}
}

func (db *TableCache) Get(k []byte) ([]byte, bool) {
	return db.impl.Get(Hash(k))
}

func (db *TableCache) Print() {
	db.impl.Print()
}

func (db *TableCache) Range(f func(k uint64, v []byte) bool) {
	db.impl.Range(f)
}

func (db *TableCache) Remove(k []byte) ([]byte, bool) {
	return db.impl.Remove(Hash(k))
}

func (db *TableCache) Set(k, v []byte) {
	db.impl.Set(Hash(k), v)
}
