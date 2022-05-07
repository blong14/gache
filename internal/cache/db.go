package cache

import (
	"bytes"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
)

type Table interface {
	Get(key []byte) ([]byte, bool)
	Set(key []byte, value []byte)
	Print()
}

type DB struct {
	impl *gtree.TreeMap[[]byte, []byte]
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.impl.Get(key)
}

func (db *DB) Set(key []byte, value []byte) {
	db.impl.Set(key, value)
}

func (db *DB) Print() {
	db.impl.Print()
}

type TableOpts struct {
	TableName []byte
	WithDB    func() *DB
	WithCache func() *gtree.TreeMap[[]byte, []byte]
}

func NewTable(o *TableOpts) Table {
	var db *gtree.TreeMap[[]byte, []byte]
	if o.WithCache != nil {
		db = o.WithCache()
	} else {
		db = gtree.New[[]byte, []byte](bytes.Compare)
	}
	return &DB{impl: db}
}
