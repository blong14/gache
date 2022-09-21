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

// table implements Table interface
type table[K, V any] struct {
	impl Table[K, V]
}

func New[K, V any](comp func(k, v K) int, eql func(k, v K) bool) Table[K, V] {
	if eql == nil {
		eql = func(k, v K) bool { return comp(k, v) == 0 }
	}
	return &table[K, V]{
		impl: gskl.New[K, V](comp, eql),
	}
}

func (db *table[K, V]) Get(k K) (V, bool) {
	return db.impl.Get(k)
}

func (db *table[K, V]) Print() {
	db.impl.Print()
}

func (db *table[K, V]) Range(f func(k K, v V) bool) {
	db.impl.Range(f)
}

func (db *table[K, V]) Remove(k K) (V, bool) {
	return db.impl.Remove(k)
}

func (db *table[K, V]) Set(k K, v V) {
	db.impl.Set(k, v)
}
