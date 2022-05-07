package actors

import (
	"context"
	"fmt"
)

type InstructionSet int

const (
	AddTable InstructionSet = iota
	GetValue
	Load
	Print
	SetValue
)

func (i InstructionSet) String() string {
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
	Inst      InstructionSet
}

type QueryResponse struct {
	Key     []byte
	Value   []byte
	Success bool
}

type Query struct {
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
	select {
	case <-ctx.Done():
	default:
		close(m.outbox)
	}
}

func NewGetValueQuery(db []byte, key []byte) (*Query, chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      GetValue,
	}
	query.Key = key
	return &query, query.outbox
}

func NewPrintQuery(db []byte) (*Query, chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      Print,
	}
	return &query, query.outbox

}

func NewLoadFromFileQuery(db []byte, filename []byte) (*Query, chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		FileName:  filename,
		Inst:      Load,
	}
	return &query, query.outbox
}

func NewSetValueQuery(db []byte, key []byte, value []byte) (*Query, chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      SetValue,
	}
	query.Key = key
	query.Value = value
	return &query, query.outbox
}

func NewAddTableQuery(db []byte) (*Query, chan *QueryResponse) {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      AddTable,
	}
	return &query, query.outbox
}
