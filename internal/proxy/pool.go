package proxy

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	gdb "github.com/blong14/gache/internal/db"
	glog "github.com/blong14/gache/internal/logging"
	gtable "github.com/blong14/gache/internal/map/tablemap"
)

type Worker struct {
	id    string
	inbox <-chan *gdb.Query
	stop  chan interface{}
	pool  *WorkPool
}

func (s *Worker) Start(ctx context.Context) {
	glog.Track("%T::%s starting", s.pool, s.id)
	for {
		select {
		case <-ctx.Done():
			glog.Track("%T::%s ctx canceled", s.pool, s.id)
			return
		case <-s.stop:
			glog.Track("%T::%s stopping", s.pool, s.id)
			return
		case query, ok := <-s.inbox:
			if !ok {
				return
			}
			start := time.Now()
			s.pool.Execute(ctx, query)
			glog.Track(
				"%T::%s executed %s %s [%s]",
				s.pool, s.id, query.Header.Inst, query.Key, time.Since(start),
			)
		}
	}
}

func (s *Worker) Stop(ctx context.Context) {
	select {
	case <-ctx.Done():
	case s.stop <- struct{}{}:
	}
}

type WorkPool struct {
	inbox chan *gdb.Query
	// table name to table view
	tables  *gtable.TableMap[[]byte, *Table]
	workers []Worker
}

func NewWorkPool(inbox chan *gdb.Query) *WorkPool {
	return &WorkPool{
		inbox:   inbox,
		tables:  gtable.New[[]byte, *Table](bytes.Compare),
		workers: make([]Worker, 0),
	}
}

func (w *WorkPool) Start(ctx context.Context) {
	for i := 0; i < runtime.NumCPU(); i++ {
		worker := Worker{
			id:    fmt.Sprintf("worker::%d", i),
			inbox: w.inbox,
			stop:  make(chan interface{}),
			pool:  w,
		}
		w.workers = append(w.workers, worker)
		go worker.Start(ctx)
	}
}

func (w *WorkPool) Send(ctx context.Context, query *gdb.Query) {
	select {
	case <-ctx.Done():
	case w.inbox <- query:
	}
}

func (w *WorkPool) Execute(ctx context.Context, query *gdb.Query) {
	switch query.Header.Inst {
	case gdb.AddTable:
		var opts *gdb.TableOpts
		if query.Header.Opts != nil {
			opts = query.Header.Opts
		} else {
			opts = &gdb.TableOpts{
				InMemory:  true,
				WalMode:   false,
				DataDir:   []byte("testdata"),
				TableName: query.Header.TableName,
			}
		}
		t := NewTable(opts)
		w.tables.Set(query.Header.TableName, t)
		query.Done(gdb.QueryResponse{Success: true})
	case gdb.Load:
		glog.Track(
			"loading csv %s for %s", query.Header.FileName, query.Header.TableName)
		loader := NewCSVReader(w)
		loader.Read(ctx, query)
	default:
		table, ok := w.tables.Get(query.Header.TableName)
		if !ok {
			query.Done(gdb.QueryResponse{Success: false})
			return
		}
		table.Execute(ctx, query)
	}
}

func (w *WorkPool) WaitAndStop(ctx context.Context) {
	glog.Track("%T stopping...\n", w)
	var wg sync.WaitGroup
	for _, worker := range w.workers {
		wg.Add(1)
		go func(w Worker) {
			defer wg.Done()
			w.Stop(ctx)
			close(w.stop)
		}(worker)
	}
	wg.Wait()
	w.tables.Range(func(k []byte, table *Table) bool {
		table.Stop()
		return true
	})
	glog.Track("%T stopped\n", w)
}
