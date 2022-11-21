package proxy

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	gcache "github.com/blong14/gache/internal/db"
	gdb "github.com/blong14/gache/internal/db"
	gtable "github.com/blong14/gache/internal/db/sorted/tablemap"
	glog "github.com/blong14/gache/internal/logging"
	gfile "github.com/blong14/gache/internal/proxy/file"
	gview "github.com/blong14/gache/internal/proxy/view"
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
				"%T::%s executed %s values=%d in %s",
				s.pool, s.id, query.Header.Inst, len(query.Values), time.Since(start),
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
	tables  gdb.Table[[]byte, *gview.Table]
	workers []Worker
}

func NewWorkPool(inbox chan *gdb.Query) *WorkPool {
	return &WorkPool{
		inbox:   inbox,
		tables:  gtable.New[[]byte, *gview.Table](bytes.Compare),
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
		var opts *gcache.TableOpts
		if query.Header.Opts != nil {
			opts = query.Header.Opts
		} else {
			opts = &gcache.TableOpts{
				TableName: query.Header.TableName,
			}
		}
		t := gview.New(opts)
		w.tables.Set(query.Header.TableName, t)
		query.Done(gdb.QueryResponse{Success: true})
	case gdb.Load:
		glog.Track(
			"loading csv %s for %s", query.Header.FileName, query.Header.TableName)
		loader := gfile.New(w)
		loader.ReadCSV(ctx, query)
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
	glog.Track("%T stopped\n", w)
}

type QueryProxy struct {
	inbox  chan *gdb.Query
	pool   *WorkPool
	tables gdb.Table[[]byte, *gview.Table]
}

func NewQueryProxy() (*QueryProxy, error) {
	inbox := make(chan *gdb.Query)
	return &QueryProxy{
		inbox:  inbox,
		pool:   NewWorkPool(inbox),
		tables: gtable.New[[]byte, *gview.Table](bytes.Compare),
	}, nil
}

func (qp *QueryProxy) Send(ctx context.Context, query *gdb.Query) {
	qp.pool.Send(ctx, query)
}

func StartProxy(ctx context.Context, qp *QueryProxy) {
	glog.Track("starting query proxy")
	qp.pool.Start(ctx)
	for _, table := range []string{"default"} {
		query, done := gdb.NewAddTableQuery(
			ctx, []byte(table),
		)
		qp.Send(ctx, query)
		<-done
	}
	glog.Track("default tables added")
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	glog.Track("stopping query proxy")
	qp.pool.WaitAndStop(ctx)
	close(qp.inbox)
}