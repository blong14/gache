package proxy

import (
	"context"
	"sync"

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

type CSVReader struct {
	worker Actor
	waiter *waiter
}

func NewCSVReader(worker Actor) *CSVReader {
	return &CSVReader{
		worker: worker,
		waiter: &waiter{chns: make([]chan gdb.QueryResponse, 0)},
	}
}

func (f *CSVReader) Read(ctx context.Context, query *gdb.Query) {
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
		var rows []gdb.KeyValue
		for _, r := range reader.Rows() {
			rows = append(rows, gdb.KeyValue{
				Key:   []byte(r[0]),
				Value: []byte(r[1]),
			})
		}
		q, done := gdb.NewBatchSetValueQuery(ctx, query.Header.TableName, rows)
		f.waiter.Add(done)
		f.worker.Send(ctx, q)
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
