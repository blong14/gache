package file

import (
	"context"
	"sync"

	gactor "github.com/blong14/gache/internal"
	gdb "github.com/blong14/gache/internal/db"
	gfile "github.com/blong14/gache/internal/io/file"
)

type waiter struct {
	sync.WaitGroup
	chns []chan gdb.QueryResponse
}

func (w *waiter) Add(ch chan gdb.QueryResponse) {
	w.chns = append(w.chns, ch)
	w.WaitGroup.Add(1)
}

func (w *waiter) Wait(ctx context.Context) {
	for _, ch := range w.chns {
		go func(done chan gdb.QueryResponse) {
			defer w.WaitGroup.Done()
			select {
			case <-ctx.Done():
			case <-done:
			}
		}(ch)
	}
	w.WaitGroup.Wait()
}

type Reader struct {
	pool   gactor.Actor
	waiter *waiter
}

func New(pool gactor.Actor) *Reader {
	return &Reader{
		pool:   pool,
		waiter: &waiter{chns: make([]chan gdb.QueryResponse, 0)},
	}
}

func (f *Reader) ReadCSV(ctx context.Context, query *gdb.Query) {
	if query.Header.Inst != gdb.Load {
		if query != nil {
			query.Done(gdb.QueryResponse{Success: false})
		}
		return
	}
	reader := gfile.ScanCSV(string(query.Header.FileName))
	defer reader.Close()
	reader.Init()
	for reader.Scan() {
		q, done := gdb.NewBatchSetValueQuery(ctx, query.Header.TableName, reader.Rows())
		f.waiter.Add(done)
		f.pool.Send(ctx, q)
	}
	f.waiter.Wait(ctx)
	success := false
	if err := reader.Err(); err == nil {
		success = true
	}
	query.Done(
		gdb.QueryResponse{
			Success: success,
			Value:   []byte("done"),
		},
	)
}

func (f *Reader) ReadDAT(ctx context.Context, query *gdb.Query) {
	if query.Header.Inst != gdb.Load {
		if query != nil {
			query.Done(gdb.QueryResponse{Success: false})
		}
		return
	}
	reader := gfile.ScanDat(string(query.Header.FileName))
	defer reader.Close()
	reader.Init()
	for reader.Scan() {
		q, done := gdb.NewBatchSetValueQuery(ctx, query.Header.TableName, reader.Rows())
		f.waiter.Add(done)
		f.pool.Send(ctx, q)
	}
	f.waiter.Wait(ctx)
	success := false
	if err := reader.Err(); err == nil {
		success = true
	}
	query.Done(
		gdb.QueryResponse{
			Success: success,
			Value:   []byte("done"),
		},
	)
}
