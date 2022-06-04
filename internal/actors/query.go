package actors

import (
	"context"
	"fmt"

	gcache "github.com/blong14/gache/internal/cache"
)

type QueryInstruction int

const (
	AddTable QueryInstruction = iota
	BatchSetValue
	GetValue
	Load
	Print
	Range
	SetValue
)

func (i QueryInstruction) String() string {
	switch i {
	case AddTable:
		return "AddTable"
	case BatchSetValue:
		return "BatchSetValue"
	case GetValue:
		return "GetValue"
	case Load:
		return "Load"
	case Print:
		return "Print"
	case Range:
		return "Range"
	case SetValue:
		return "SetValue"
	default:
		return "unknown"
	}
}

type QueryHeader struct {
	TableName []byte
	Opts      *gcache.TableOpts
	FileName  []byte
	Inst      QueryInstruction
}

type QueryResponse struct {
	Key     []byte
	Value   []byte
	Success bool
}

type Query struct {
	ctx    context.Context
	done   chan QueryResponse
	Header QueryHeader
	Key    []byte
	Value  []byte
	Values []KeyValue
}

func NewQuery(ctx context.Context, outbox chan QueryResponse) *Query {
	if outbox == nil {
		q := TraceNewQuery(ctx)
		return &q
	}
	return &Query{ctx: ctx, done: outbox}
}

func TraceNewQuery(ctx context.Context) Query {
	return Query{
		ctx:  ctx,
		done: make(chan QueryResponse, 1),
	}
}

func (m *Query) String() string {
	return fmt.Sprintf("%s %s %s", m.Header.TableName, m.Header.Inst, m.Key)
}

func (m *Query) Done(r QueryResponse) {
	select {
	case m.done <- r:
	default:
	}
}

func (m *Query) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *Query) GetResponse() *QueryResponse {
	resp := <-m.done
	return &resp
}

func NewGetValueQuery(ctx context.Context, db []byte, key []byte) (*Query, chan QueryResponse) {
	done := make(chan QueryResponse, 1)
	query := NewQuery(ctx, done)
	query.Header = QueryHeader{
		TableName: db,
		Inst:      GetValue,
	}
	query.Key = key
	return query, done
}

func NewPrintQuery(ctx context.Context, db []byte) (*Query, chan QueryResponse) {
	done := make(chan QueryResponse, 1)
	query := NewQuery(ctx, done)
	query.Header = QueryHeader{
		TableName: db,
		Inst:      Print,
	}
	return query, done
}

func NewRangeQuery(ctx context.Context, db []byte) (*Query, chan QueryResponse) {
	done := make(chan QueryResponse, 1)
	query := NewQuery(ctx, done)
	query.Header = QueryHeader{
		TableName: db,
		Inst:      Range,
	}
	return query, done
}

func NewLoadFromFileQuery(ctx context.Context, db []byte, filename []byte) (*Query, chan QueryResponse) {
	done := make(chan QueryResponse, 1)
	query := NewQuery(ctx, done)
	query.Header = QueryHeader{
		TableName: db,
		FileName:  filename,
		Inst:      Load,
	}
	return query, done
}

func NewSetValueQuery(ctx context.Context, db, key, value []byte) (*Query, chan QueryResponse) {
	done := make(chan QueryResponse, 1)
	query := NewQuery(ctx, done)
	query.Header = QueryHeader{
		TableName: db,
		Inst:      SetValue,
	}
	query.Key = key
	query.Value = value
	return query, done
}

func NewBatchSetValueQuery(ctx context.Context, db []byte, values []KeyValue) (*Query, chan QueryResponse) {
	done := make(chan QueryResponse, 1)
	query := NewQuery(ctx, done)
	query.Header = QueryHeader{
		TableName: db,
		Inst:      BatchSetValue,
	}
	query.Values = values
	return query, done
}

func NewAddTableQuery(ctx context.Context, db []byte) (*Query, chan QueryResponse) {
	done := make(chan QueryResponse, 1)
	query := NewQuery(ctx, done)
	query.Header = QueryHeader{
		TableName: db,
		Inst:      AddTable,
	}
	return query, done
}
