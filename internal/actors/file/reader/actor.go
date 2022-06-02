package reader

import (
	"context"
	"log"
	"sync"

	gactors "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/io/file"
	glog "github.com/blong14/gache/internal/logging"
)

// Reader implements Actor interface
type Reader struct {
	done  chan struct{}
	inbox chan *gactors.Query
	table gactors.Actor
	scnr  *gfile.Scanner
}

func New(table gactors.Actor) gactors.Actor {
	return &Reader{
		inbox: make(chan *gactors.Query),
		done:  make(chan struct{}),
		table: table,
		scnr:  gfile.NewScanner(),
	}
}

func (f *Reader) Init(ctx context.Context) {
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
				q, done := gactors.NewBatchSetValueQuery(ctx, query.Header.TableName, f.scnr.Rows())
				f.table.Execute(q.Context(), q)
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

func (f *Reader) Close(_ context.Context) {
	close(f.done)
}

func (f *Reader) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-f.done:
	case <-ctx.Done():
	case f.inbox <- query:
	}
}
