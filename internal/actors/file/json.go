package file

import (
	"context"
	"log"
	"sync"

	gactors "github.com/blong14/gache/internal/actors"
	gjson "github.com/blong14/gache/internal/io/json"
	glog "github.com/blong14/gache/logging"
)

func drain(ctx context.Context, wg *sync.WaitGroup, result <-chan *gactors.QueryResponse) {
	defer wg.Done()
	select {
	case <-ctx.Done():
	case <-result:
	}
}

// implements Actor interface
type loader struct {
	done   chan struct{}
	inbox  chan *gactors.Query
	outbox chan []*gactors.Query
}

func New() gactors.Streamer {
	return &loader{
		inbox:  make(chan *gactors.Query),
		outbox: make(chan []*gactors.Query),
	}
}

func (f *loader) Start(ctx context.Context) {
	glog.Track("%T waiting for work", f)
	defer glog.Track("%T stopped", f)
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.done:
			return
		case query, ok := <-f.inbox:
			if !ok {
				if query != nil {
					query.Finish(ctx)
				}
				return
			}
			switch query.Header.Inst {
			case gactors.Load:
				done, err := gjson.ReadJSON(ctx, string(query.Header.FileName))
				if err != nil {
					log.Fatal(err)
				}
				var wg sync.WaitGroup
				buffer := make([]*gactors.Query, 0, 20000)
				for row := range done {
					for _, kv := range row {
						wg.Add(1)
						setValue, result := gactors.NewSetValueQuery(query.Header.TableName, kv.Key, kv.Value)
						go drain(ctx, &wg, result)
						buffer = append(buffer, setValue)
					}
				}
				go func() {
					defer query.Finish(ctx)
					wg.Wait()
					query.OnResult(ctx, gactors.QueryResponse{Success: true})
				}()
				select {
				case <-ctx.Done():
					query.Finish(ctx)
					return
				case f.outbox <- buffer:
				}
			}
		}
	}
}

func (f *loader) Stop(_ context.Context) {
	close(f.done)
	close(f.inbox)
	close(f.outbox)
}

func (f *loader) OnResult() <-chan []*gactors.Query {
	out := make(chan []*gactors.Query)
	go func() {
		defer close(out)
		query := <-f.outbox
		out <- query
	}()
	return out
}

func (f *loader) Execute(ctx context.Context, row *gactors.Query) {
	select {
	case <-ctx.Done():
	case <-f.done:
	case f.inbox <- row:
	}
}