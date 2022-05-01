package cache

import (
	gtree "github.com/blong14/gache/internal/cache/sorted/tablemap"
)

type DB struct {
	impl *gtree.TableMap[[]byte, []byte]
}

type DBOpts struct {
	Impl *gtree.TableMap[[]byte, []byte]
}

func NewDB(o *DBOpts) *DB {
	var impl *gtree.TableMap[[]byte, []byte]
	if o.Impl != nil {
		impl = o.Impl
	}
	return &DB{impl}
}

func (db *DB) get(key []byte) ([]byte, bool) {
	return db.impl.Get(key)
}

type Reader interface {
	Get(key []byte) ([]byte, bool)
}

type View struct {
	table *DB
}

type ViewOpts struct {
	WithDB func() *DB
}

func NewView(o *ViewOpts) Reader {
	var db *DB
	if o.WithDB != nil {
		db = o.WithDB()
	}
	return &View{table: db}
}

func (v *View) Get(key []byte) ([]byte, bool) {
	return v.table.get(key)
}

type Writer interface {
	Set(key []byte, value []byte) error
}
