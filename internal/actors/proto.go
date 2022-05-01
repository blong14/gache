package actors

import "context"

type Response struct {
	Key     []byte
	Value   []byte
	Success bool
}

type RangeQuery struct {
	Start []byte
	End   []byte
}

type Query struct {
	CMD        InstructionSet
	Key        []byte
	Value      []byte
	RangeQuery *RangeQuery
	Index      IndexActor
	done       chan Response
}

func (m *Query) Finish() {
	close(m.done)
}

func NewQuery() Query {
	return Query{done: make(chan Response), RangeQuery: nil}
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

// OnResult block and waits until the r Response is read via
// the Result method
func (m *Query) OnResult(ctx context.Context, r Response) {
	select {
	case <-ctx.Done():
		return
	case m.done <- r:
	}
}

type InstructionSet int

const (
	AddIndex InstructionSet = iota
	GetValue
)
