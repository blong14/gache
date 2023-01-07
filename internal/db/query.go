package db

import (
	"context"
	"fmt"
)

type QueryInstruction int

const (
	AddTable QueryInstruction = iota
	BatchSetValue
	Count
	GetValue
	GetRange
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
	case Count:
		return "Count"
	case GetValue:
		return "GetValue"
	case GetRange:
		return "GetRange"
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
	DataDir   []byte
	TableName []byte
	Opts      *TableOpts
	FileName  []byte
	Inst      QueryInstruction
}

type QueryStats struct {
	Count uint
}

type QueryResponse struct {
	Key         []byte
	Value       []byte
	RangeValues [][][]byte
	Stats       QueryStats
	Success     bool
}

type KeyRange struct {
	Start []byte
	End   []byte
	Limit int
}

func (kr *KeyRange) String() string {
	return fmt.Sprintf("%s %s", kr.Start, kr.End)
}

type Query struct {
	ctx      context.Context
	done     chan QueryResponse
	Header   QueryHeader
	KeyRange KeyRange
	Key      []byte
	Value    []byte
	Values   []KeyValue
}

func NewQuery(ctx context.Context, outbox chan QueryResponse) *Query {
	if outbox == nil {
		outbox = make(chan QueryResponse, 1)
	}
	return &Query{ctx: ctx, done: outbox}
}

func (m *Query) String() string {
	return fmt.Sprintf(
		"%s %s %s %s %s %s",
		m.Header.FileName, m.Header.TableName,
		m.Header.Inst, m.Key, m.Value, m.KeyRange.String(),
	)
}

func (m *Query) Done(r QueryResponse) {
	select {
	case <-m.ctx.Done():
	case m.done <- r:
		close(m.done)
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
		Inst:      GetRange,
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

func XNewAddTableQuery(ctx context.Context, dir, db []byte) (*Query, chan QueryResponse) {
	done := make(chan QueryResponse, 1)
	query := NewQuery(ctx, done)
	query.Header = QueryHeader{
		DataDir:   dir,
		TableName: db,
		Inst:      AddTable,
	}
	return query, done
}
