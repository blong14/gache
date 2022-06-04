package cache

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
)

type Table[K, V any] interface {
	Get(k K) (V, bool)
	Print()
	Range(func(k K, v V) bool)
	Remove(k K) (V, bool)
	Set(k K, v V)
}

type TableTracer[K, V any] interface {
	Table[K, V]
	TraceSet(ctx context.Context, key []byte, value []byte)
}

type TableOpts struct {
	TableName []byte
}

type table[K, V any] struct {
	impl   Table[K, V]
	tracer trace.Tracer
}

func XNew[K, V any](comp func(k, v K) int, eql func(k, v K) bool) Table[K, V] {
	if eql == nil {
		eql = func(k, v K) bool { return comp(k, v) == 0 }
	}
	return &table[K, V]{
		impl: &gskl.SkipList[K, V]{
			Comparator: comp,
			Matcher:    eql,
			Sentinal:   gskl.NewMapEntry[K, V](*new(K), *new(V)),
			MaxHeight:  gskl.MaxHeight,
			H:          uint64(0),
		},
		tracer: otel.Tracer("skiplist"),
	}
}

func (db *table[K, V]) Get(key K) (V, bool) {
	return db.impl.Get(key)
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

func (db *table[K, V]) TraceSet(ctx context.Context, k K, v V) {
	_, span := db.tracer.Start(ctx, "skiplist:set")
	defer span.End()
	db.impl.Set(k, v)
}
