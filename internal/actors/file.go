package actors

import (
	"context"

	gcsv "github.com/blong14/gache/internal/io/csv"
)

type Streamer interface {
	Actor
	OnResult() <-chan *Query
}

// implements Actor interface
type fileActor struct {
	inbox  chan *Query
	outbox chan *Query
}

func (f *fileActor) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case query := <-f.inbox:
			switch query.Header.Inst {
			case Load:
				for row := range gcsv.Read(string(query.Header.FileName)) {
					if len(row) != 2 {
						continue
					}
					setValue, done := NewSetValueQuery(query.Header.TableName, []byte(row[0]), []byte(row[1]))
					go func() { <-done }()
					select {
					case <-ctx.Done():
						return
					case f.outbox <- setValue:
					}
				}
				query.OnResult(ctx, QueryResponse{Success: true})
				query.Finish()
			}
		}
	}
}

func (f *fileActor) OnResult() <-chan *Query {
	return f.outbox
}

func (f *fileActor) Stop(_ context.Context) {
	close(f.inbox)
	close(f.outbox)
}

func (f *fileActor) Execute(ctx context.Context, row *Query) {
	select {
	case <-ctx.Done():
	case f.inbox <- row:
	}
}

func NewFileActor() Streamer {
	return &fileActor{
		inbox:  make(chan *Query),
		outbox: make(chan *Query),
	}
}
