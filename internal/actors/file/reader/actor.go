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
			data, err := gfile.ReadCSV(string(query.Header.FileName))
			if err != nil {
				log.Fatal(err)
			}
			buffer := make([]gfile.KeyValue, 0, len(data))
			for _, d := range data {
				buffer = append(buffer, d)
				if len(buffer) >= 1000 {
					select {
					case <-ctx.Done():
					case <-f.done:
					case f.outbox <- buffer:
						buffer = []gfile.KeyValue{}
					}
				}
			}
			if len(buffer) > 0 {
				select {
				case <-ctx.Done():
				case <-f.done:
				case f.outbox <- buffer:
				}
			}
			query.OnResult(ctx, gactors.QueryResponse{Success: true})
			query.Finish(ctx)
			f.Close(ctx)
			return
		}
	}
}

func (f *loader) Close(_ context.Context) {
	close(f.done)
	close(f.inbox)
	close(f.outbox)
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
	case <-ctx.Done():
	case <-f.done:
	case f.inbox <- query:
	}
}
