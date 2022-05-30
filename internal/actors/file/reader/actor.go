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
	done  chan struct{}
	inbox chan *gactors.Query
	table gactors.Actor
	scnr  *gfile.Scanner
}

func New(table gactors.Actor) gactors.Actor {
	return &loader{
		inbox: make(chan *gactors.Query),
		done:  make(chan struct{}),
		table: table,
		scnr:  gfile.NewScanner(),
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
			if !ok || f.table == nil || query.Header.Inst != gactors.Load {
				if query != nil {
					query.Done(gactors.QueryResponse{Success: false})
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
				query, done := gactors.NewBatchSetValueQuery(ctx, []byte("default"), f.scnr.Rows())
				f.table.Execute(query.Context(), query)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer close(done)
					select {
					case <-ctx.Done():
					case <-done:
					}
				}()
			}
			wg.Wait()
			query.Done(gactors.QueryResponse{Success: true})
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
