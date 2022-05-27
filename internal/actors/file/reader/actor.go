package reader

import (
	"context"
	gactors "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/io/file"
	glog "github.com/blong14/gache/internal/logging"
	"log"
)

// implements Actor interface
type loader struct {
	done   chan struct{}
	inbox  chan *gactors.Query
	outbox chan []gfile.KeyValue
}

func New() gactors.Streamer {
	return &loader{
		inbox:  make(chan *gactors.Query),
		outbox: make(chan []gfile.KeyValue),
		done:   make(chan struct{}),
	}
}

func (f *loader) Init(ctx context.Context) {
	glog.Track("%T waiting for work", f)
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.done:
			return
		case query, ok := <-f.inbox:
			if !ok || query.Header.Inst != gactors.Load {
				if query != nil {
					query.Finish(ctx)
				}
				return
			}
			buffer, err := gfile.ReadCSV(string(query.Header.FileName))
			if err != nil {
				log.Fatal(err)
			}
			if len(buffer) > 0 {
				select {
				case <-ctx.Done():
				case <-f.done:
				case f.outbox <- buffer:
				}
			}
			f.Close(query.Context())
			return
		}
	}
}

func (f *loader) Close(_ context.Context) {
	close(f.done)
}

func (f *loader) OnResult() <-chan []*gactors.Query {
	out := make(chan []*gactors.Query)
	go func() {
		defer close(out)
		for {
			select {
			case <-f.done:
				return
			case data := <-f.outbox:
				buffer := make([]*gactors.Query, 0, len(data))
				for _, d := range data {
					query, _ := gactors.NewSetValueQuery(
						[]byte("default"),
						d.Key,
						d.Value,
					)
					buffer = append(buffer, query)
				}
				select {
				case <-f.done:
					return
				case out <- buffer:
				}
			}
		}
	}()
	return out
}

func (f *loader) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-f.done:
	case <-ctx.Done():
	case f.inbox <- query:
	}
}
