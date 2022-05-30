package reader

import (
	"context"
	"log"
	"sync"

	gactors "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/io/file"
	glog "github.com/blong14/gache/internal/logging"
)

// implements Actor interface
type loader struct {
	done   chan struct{}
	inbox  chan *gactors.Query
	outbox chan []gactors.KeyValue
	Table  gactors.Actor
	scnr   *gfile.Scanner
}

func New(table gactors.Actor) gactors.Actor {
	return &loader{
		inbox:  make(chan *gactors.Query),
		outbox: make(chan []gactors.KeyValue),
		done:   make(chan struct{}),
		Table:  table,
		scnr:   gfile.NewScanner(),
	}
}

func (f *loader) Init(ctx context.Context) {
	glog.Track("%T waiting for work", f)
	defer f.Close(ctx)
	for {
		select {
		case <-f.done:
			return
		case <-ctx.Done():
			return
		case query, ok := <-f.inbox:
			if !ok || f.Table == nil || query.Header.Inst != gactors.Load {
				if query != nil {
					query.OnResult(ctx, gactors.QueryResponse{Success: false})
					query.Finish(ctx)
				}
				return
			}
			buffer, err := gfile.ReadCSV(string(query.Header.FileName))
			if err != nil {
				log.Fatal(err)
			}
			f.scnr.Init(buffer)
			var wg sync.WaitGroup
			for f.scnr.Scan() {
				wg.Add(1)
				query, done := gactors.NewBatchSetValueQuery([]byte("default"), f.scnr.Rows())
				f.Table.Execute(ctx, query)
				go func() {
					defer wg.Done()
					select {
					case <-ctx.Done():
					case <-done:
					}
				}()
			}
			wg.Wait()
			query.OnResult(ctx, gactors.QueryResponse{Success: true})
			query.Finish(ctx)
			return
		}
	}
}

func (f *loader) Close(_ context.Context) {
	close(f.done)
}

func (f *loader) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-f.done:
	case <-ctx.Done():
	case f.inbox <- query:
	}
}
