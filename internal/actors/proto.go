package actors

import (
	"context"
	"fmt"
)

type InstructionSet int

const (
	AddTable InstructionSet = iota
	GetValue
	SetValue
)

func (i InstructionSet) String() string {
	switch i {
	case AddTable:
		return "AddTable"
	case GetValue:
		return "GetValue"
	case SetValue:
		return "SetValue"
	default:
		return "unknown"
	}
}

type QueryHeader struct {
	TableName []byte
	Inst      InstructionSet
}

type QueryResponse struct {
	Key     []byte
	Value   []byte
	Success bool
}

type Query struct {
	done   chan struct{}
	outbox chan *QueryResponse
	Header QueryHeader
	Key    []byte
	Value  []byte
}

func NewQuery() Query {
	return Query{
		done:   make(chan struct{}),
		outbox: make(chan *QueryResponse, 1),
	}
}

func (m *Query) String() string {
	return fmt.Sprintf("%s %s %s", m.Header.TableName, m.Header.Inst, m.Key)
}

func (m *Query) OnResult(ctx context.Context, r QueryResponse) {
	select {
	case <-ctx.Done():
	case <-m.done:
	case m.outbox <- &r:
	}
}

func (m *Query) Finish() {
	close(m.done)
	close(m.outbox)
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
