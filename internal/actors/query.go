package actors

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel"
)

type QueryInstruction int

const (
	AddTable QueryInstruction = iota
	GetValue
	Load
	Print
	SetValue
)

func (i QueryInstruction) String() string {
	switch i {
	case AddTable:
		return "AddTable"
	case GetValue:
		return "GetValue"
	case Load:
		return "Load"
	case Print:
		return "Print"
	case SetValue:
		return "SetValue"
	default:
		return "unknown"
	}
}

type QueryHeader struct {
	TableName []byte
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
	outbox chan *QueryResponse
	Header QueryHeader
	Key    []byte
	Value  []byte
}

func NewQuery() Query {
	return Query{
		outbox: make(chan *QueryResponse),
	}
}

func TraceNewQuery(ctx context.Context) Query {
	return Query{
		ctx:    ctx,
		outbox: make(chan *QueryResponse),
	}
}

func (m *Query) String() string {
	return fmt.Sprintf("%s %s %s", m.Header.TableName, m.Header.Inst, m.Key)
}

func (m *Query) OnResult(ctx context.Context, r QueryResponse) {
	select {
	case <-ctx.Done():
	case m.outbox <- &r:
	}
}

func (m *Query) Finish(ctx context.Context) {
	if m.outbox == nil {
		return
	}
	select {
	case <-ctx.Done():
	default:
		close(m.outbox)
	}
}

func (m *Query) Context() context.Context {
	return m.ctx
}

func NewGetValueQuery(db []byte, key []byte) (*Query, <-chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      GetValue,
	}
	query.Key = key
	return &query, query.outbox
}

func NewPrintQuery(db []byte) (*Query, <-chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      Print,
	}
	return &query, query.outbox

}

func NewLoadFromFileQuery(db []byte, filename []byte) (*Query, <-chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		FileName:  filename,
		Inst:      Load,
	}
	return &query, query.outbox
}

func TraceNewLoadFromFileQuery(ctx context.Context, db []byte, filename []byte) (*Query, <-chan *QueryResponse) {
	query, outbox := NewLoadFromFileQuery(db, filename)
	query.ctx = ctx
	return query, outbox
}

func NewSetValueQuery(db []byte, key []byte, value []byte) (*Query, <-chan *QueryResponse) {
	ctx := context.Background()
	query := TraceNewQuery(ctx)
	query.Header = QueryHeader{
		TableName: db,
		Inst:      SetValue,
	}
	query.Key = key
	query.Value = value
	return &query, query.outbox
}

func TraceNewSetValueQuery(ctx context.Context, db []byte, key []byte, value []byte) (*Query, <-chan *QueryResponse) {
	query, outbox := NewSetValueQuery(db, key, value)
	query.ctx = ctx
	return query, outbox
}

func NewAddTableQuery(db []byte) (*Query, <-chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      AddTable,
	}
	return &query, query.outbox
}

func TraceNewAddTableQuery(ctx context.Context, db []byte) (*Query, <-chan *QueryResponse) {
	query := NewQuery()
	query.ctx = ctx
	query.Header = QueryHeader{
		TableName: db,
		Inst:      AddTable,
	}
	return &query, query.outbox
}

func GetQueryResult(ctx context.Context, query *Query) *QueryResponse {
	_, span := otel.Tracer("").Start(ctx, "query:GetQueryResult")
	defer span.End()
	var result *QueryResponse
	select {
	case <-ctx.Done():
	case result = <-query.outbox:
	}
	return result
}
