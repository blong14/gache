package cache

import (
	"bytes"
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
)

type Table interface {
	Get(key []byte) ([]byte, bool)
	Print()
	Range(f func(k, v any) bool)
	Set(key []byte, value []byte)
}

type TableTracer interface {
	Table
	TraceSet(ctx context.Context, key []byte, value []byte)
}

type TableOpts struct {
	TableName    []byte
	WithTreeMap  func() *gtree.TreeMap[[]byte, []byte]
	WithSkipList func() *gskl.SkipList[[]byte, []byte]
}

// SkipListDB implements Table and TableTracer
type SkipListDB struct {
	impl   *gskl.SkipList[[]byte, []byte]
	tracer trace.Tracer
}

var _ TableTracer = &SkipListDB{}

func NewSkipListDB(opt *TableOpts) Table {
	var impl *gskl.SkipList[[]byte, []byte]
	if opt != nil && opt.WithSkipList != nil {
		impl = opt.WithSkipList()
	} else {
		impl = gskl.New[[]byte, []byte](bytes.Compare, bytes.Equal)
	}
	return &SkipListDB{impl: impl, tracer: otel.Tracer("skiplist")}
}

func (db *SkipListDB) Get(key []byte) ([]byte, bool) {
	return db.impl.Get(key)
}

func (db *SkipListDB) Print() {
	db.impl.Print()
}

func (db *SkipListDB) Range(f func(k, v any) bool) {
	db.impl.Range(f)
}

func (db *SkipListDB) Set(key []byte, value []byte) {
	db.impl.Set(key, value)
}

func (db *SkipListDB) TraceSet(ctx context.Context, key []byte, value []byte) {
	_, span := db.tracer.Start(ctx, "skiplist:set")
	defer span.End()
	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.String("value", string(value)),
	)
	db.impl.Set(key, value)
}

// TreeMapDB implements Table and TableTracer
type TreeMapDB struct {
	mtx    sync.RWMutex
	impl   *gtree.TreeMap[[]byte, []byte]
	tracer trace.Tracer
}

var _ TableTracer = &TreeMapDB{}

func NewTreeMapDB(opt *TableOpts) Table {
	var impl *gtree.TreeMap[[]byte, []byte]
	if opt != nil && opt.WithTreeMap != nil {
		impl = opt.WithTreeMap()
	} else {
		impl = gtree.New[[]byte, []byte](bytes.Compare)
	}
	return &TreeMapDB{impl: impl, mtx: sync.RWMutex{}, tracer: otel.Tracer("treemap")}
}

func (db *TreeMapDB) Get(key []byte) ([]byte, bool) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	return db.impl.Get(key)
}

func (db *TreeMapDB) Print() {
	db.impl.Print()
}

func (db *TreeMapDB) Range(f func(k, v any) bool) {
	db.impl.Range(f)
}
func (db *TreeMapDB) Set(key []byte, value []byte) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	db.impl.Set(key, value)
}

func (db *TreeMapDB) TraceSet(ctx context.Context, key []byte, value []byte) {
	_, span := db.tracer.Start(ctx, "treemap:set")
	defer span.End()
	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.String("value", string(value)),
	)
	db.mtx.Lock()
	db.impl.Set(key, value)
	db.mtx.Unlock()
}
