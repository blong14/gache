package cache

import (
	"bytes"
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	genv "github.com/blong14/gache/internal/environment"
)

type Table interface {
	Get(key []byte) ([]byte, bool)
	Set(key []byte, value []byte)
	TraceSet(ctx context.Context, key []byte, value []byte)
	Print()
	Range(ctx context.Context)
}

type XDB struct {
	impl *gtree.TreeMap[[]byte, []byte]
}

func (db *XDB) Get(key []byte) ([]byte, bool) {
	return db.impl.Get(key)
}

func (db *XDB) Set(key []byte, value []byte) {
	db.impl.Set(key, value)
}

func (db *XDB) TraceSet(ctx context.Context, key []byte, value []byte) {
	if genv.TraceEnabled() {
		_, span := otel.Tracer("").Start(ctx, "db:set")
		defer span.End()
	}
	db.impl.Set(key, value)
}

func (db *XDB) Print() {
	db.impl.Print()
}

func (db *XDB) Range(ctx context.Context) {
	db.impl.Print()
}

type DB struct {
	impl *gskl.SkipList[[]byte, []byte]
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.impl.Get(key)
}

func (db *DB) Set(key []byte, value []byte) {
	db.impl.Set(key, value)
}

func (db *DB) TraceSet(ctx context.Context, key []byte, value []byte) {
	if genv.TraceEnabled() {
		_, span := otel.Tracer("").Start(ctx, "db:set")
		defer span.End()
	}
	db.impl.Set(key, value)
}

func (db *DB) Range(ctx context.Context) {
	db.impl.Range(func(k, v any) bool {
		fmt.Printf("%s", k)
		return true
	})
}

func (db *DB) Print() {
	db.impl.Print()
}

type TableOpts struct {
	Concurrent   bool
	TableName    []byte
	WithDB       func() *DB
	WithCache    func() *gtree.TreeMap[[]byte, []byte]
	WithSkipList func() *gskl.SkipList[[]byte, []byte]
}

func NewTable(o *TableOpts) Table {
	var db *gtree.TreeMap[[]byte, []byte]
	if o.WithCache != nil {
		db = o.WithCache()
		return &XDB{impl: db}
	}
	var xdb *gskl.SkipList[[]byte, []byte]
	if o.WithSkipList != nil {
		xdb = o.WithSkipList()
	} else {
		xdb = gskl.XNew[[]byte, []byte](bytes.Compare, bytes.Equal)
	}
	return &DB{impl: xdb}
}
