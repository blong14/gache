package actors

import "context"

type InstructionSet int

const (
	AddTable InstructionSet = iota
	GetValue
	SetValue
)

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
	Header QueryHeader
	Key    []byte
	Value  []byte
	done   chan QueryResponse
}

func NewQuery() Query {
	return Query{
		done: make(chan QueryResponse),
	}
}

// OnResult block and waits until the r QueryResponse is read via
// the Result method
func (m *Query) OnResult(ctx context.Context, r QueryResponse) {
	select {
	case <-ctx.Done():
		return
	case m.done <- r:
	}
}

// Result blocks and waits for a response on the message's done channel
// and returns a slice of bytes and the message's status
func (m *Query) Result(ctx context.Context) ([]byte, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case resp := <-m.done:
		return resp.Value, resp.Success
	}
}

func (m *Query) Finish() {
	close(m.done)
}

func NewGetValueQuery(db []byte, key []byte) *Query {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      GetValue,
	}
	query.Key = key
	return &query
}

func NewSetValueQuery(db []byte, key []byte, value []byte) *Query {
	query := NewQuery()
	query.Header = QueryHeader{
		TableName: db,
		Inst:      SetValue,
	}
	query.Key = key
	query.Value = value
	return &query
}
