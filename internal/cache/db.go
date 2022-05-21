package cache

import (
	"bytes"
	"context"

	"go.opentelemetry.io/otel"

	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
)

type Table interface {
	Get(key []byte) ([]byte, bool)
	Set(key []byte, value []byte)
	TraceSet(ctx context.Context, key []byte, value []byte)
	Print()
}

type DB struct {
	ximpl *gtree.TreeMap[[]byte, []byte]
	impl  *gskl.SkipList[[]byte, []byte]
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.impl.Get(key)
}

func (db *DB) Set(key []byte, value []byte) {
	db.impl.Set(key, value)
}

func (db *DB) TraceSet(ctx context.Context, key []byte, value []byte) {
	_, span := otel.Tracer("").Start(ctx, "db:set")
	defer span.End()
	db.impl.Set(key, value)
}

func (db *DB) Print() {
	db.impl.Print()
}

type TableOpts struct {
	TableName    []byte
	WithDB       func() *DB
	WithCache    func() *gtree.TreeMap[[]byte, []byte]
	WithSkipList func() *gskl.SkipList[[]byte, []byte]
}

func NewTable(o *TableOpts) Table {
	var db *gtree.TreeMap[[]byte, []byte]
	if o.WithCache != nil {
		db = o.WithCache()
	} else {
		db = gtree.New[[]byte, []byte](bytes.Compare)
	}
	var xdb *gskl.SkipList[[]byte, []byte]
	if o.WithSkipList != nil {
		xdb = o.WithSkipList()
	} else {
		xdb = gskl.New[[]byte, []byte](bytes.Compare)
	}
	return &DB{ximpl: db, impl: xdb}
}
