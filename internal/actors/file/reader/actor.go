package reader

import (
	"context"
	"sync"

	gactors "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/io/file"
)

type waiter struct {
	sync.WaitGroup
	chns []chan gactors.QueryResponse
}

func (w *waiter) Add(ch chan gactors.QueryResponse) {
	w.chns = append(w.chns, ch)
	w.WaitGroup.Add(1)
}

func (w *waiter) Wait(ctx context.Context) {
	for _, ch := range w.chns {
		go func(done chan gactors.QueryResponse) {
			defer w.WaitGroup.Done()
			select {
			case <-ctx.Done():
			case <-done:
			}
		}(ch)
	}
	w.WaitGroup.Wait()
}

// Reader implements Actor interface
type Reader struct {
	pool   gactors.Actor
	waiter *waiter
}

func New(pool gactors.Actor) gactors.Actor {
	return &Reader{
		pool:   pool,
		waiter: &waiter{chns: make([]chan gactors.QueryResponse, 0)},
	}
}

func (f *Reader) Send(ctx context.Context, query *gactors.Query) {
	if query.Header.Inst != gactors.Load {
		if query != nil {
			query.Done(gactors.QueryResponse{Success: false})
		}
		return
	}
	reader := gfile.ScanCSV(string(query.Header.FileName))
	defer reader.Close()
	reader.Init()
	for reader.Scan() {
		q, done := gactors.NewBatchSetValueQuery(ctx, query.Header.TableName, reader.Rows())
		f.waiter.Add(done)
		f.pool.Send(ctx, q)
	}
	f.waiter.Wait(ctx)
	success := false
	if err := reader.Err(); err == nil {
		success = true
	}
	query.Done(
		gactors.QueryResponse{
			Success: success,
			Value:   []byte("done"),
		},
	)
}
