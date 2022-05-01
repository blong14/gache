package cache

import (
	gtree "github.com/blong14/gache/internal/cache/sorted/tablemap"
)

type Table interface {
	Get(key []byte) ([]byte, bool)
	Set(key []byte, value []byte)
}

type DB struct {
	impl *gtree.TableMap[[]byte, []byte]
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.impl.Get(key)
}

func (db *DB) Set(key []byte, value []byte) {
	db.impl.Set(key, value)
}

type TableOpts struct {
	WithDB    func() *DB
	WithCache func() *gtree.TableMap[[]byte, []byte]
}

func NewTable(o *TableOpts) Table {
	var db *gtree.TableMap[[]byte, []byte]
	if o.WithCache != nil {
		db = o.WithCache()
	}
	return &DB{impl: db}
}
