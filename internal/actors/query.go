package actors

import (
	"context"
	"fmt"
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
	Values []KeyValue
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
	if m.ctx == nil {
		return context.Background()
	}
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

func NewRangeQuery(db []byte) (*Query, <-chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      Range,
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

func NewBatchSetValueQuery(db []byte, values []KeyValue) (*Query, <-chan *QueryResponse) {
	ctx := context.Background()
	query := TraceNewQuery(ctx)
	query.Header = QueryHeader{
		TableName: db,
		Inst:      BatchSetValue,
	}
	query.Values = values
	return &query, query.outbox
}

func NewAddTableQuery(db []byte) (*Query, <-chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      AddTable,
	}
	return &query, query.outbox
}

func GetQueryResult(ctx context.Context, query *Query) *QueryResponse {
	var result *QueryResponse
	select {
	case <-ctx.Done():
	case result = <-query.outbox:
	}
	return result
}
