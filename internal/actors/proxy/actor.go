package proxy

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	gactor "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/actors/file/reader"
	gview "github.com/blong14/gache/internal/actors/view"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
	glog "github.com/blong14/gache/internal/logging"
)

type Worker struct {
	id      string
	healthz chan struct{}
	inbox   <-chan *gactor.Query
	stop    chan interface{}
	pool    *WorkPool
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
	inbox chan *gactor.Query
	log   *gwal.Log
	// table name to table actor
	tables  gcache.Table[[]byte, gactor.Actor]
	healthz chan struct{}
	workers []Worker
}

func NewWorkPool(log *gwal.Log, inbox chan *gactor.Query) *WorkPool {
	return &WorkPool{
		inbox:   inbox,
		healthz: make(chan struct{}, 1),
		log:     log,
		tables:  gskl.New[[]byte, gactor.Actor](bytes.Compare),
		workers: make([]Worker, 0),
	}
}

func (w *WorkPool) Start(ctx context.Context) {
	for i := 0; i < runtime.NumCPU(); i++ {
		worker := Worker{
			id:      fmt.Sprintf("worker::%d", i),
			inbox:   w.inbox,
			stop:    make(chan interface{}, 0),
			healthz: w.healthz,
			pool:    w,
		}
		w.workers = append(w.workers, worker)
		go worker.Start(ctx)
	}
}

func (w *WorkPool) Send(ctx context.Context, query *gactor.Query) {
	select {
	case <-ctx.Done():
	case w.inbox <- query:
	}
}

func (w *WorkPool) Execute(ctx context.Context, query *gactor.Query) {
	switch query.Header.Inst {
	case gactor.AddTable:
		var opts *gcache.TableOpts
		if query.Header.Opts != nil {
			opts = query.Header.Opts
		} else {
			opts = &gcache.TableOpts{
				TableName: query.Header.TableName,
			}
		}
		t := gview.New(w.log, opts)
		w.tables.Set(query.Header.TableName, t)
		w.log.Send(ctx, query)
		query.Done(gactor.QueryResponse{Success: true})
	case gactor.Load:
		glog.Track(
			"loading csv %s for %s", query.Header.FileName, query.Header.TableName)
		loader := gfile.New(w)
		loader.Send(ctx, query)
	default:
		table, ok := w.tables.Get(query.Header.TableName)
		if !ok {
			query.Done(gactor.QueryResponse{Success: false})
			return
		}
		table.Send(ctx, query)
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

// QueryProxy implements actors.Actor interface
type QueryProxy struct {
	inbox chan *gactor.Query
	pool  *WorkPool
	// table name to table actor
	tables gcache.Table[[]byte, gactor.Actor]
}

// NewQueryProxy returns a fully ready to use *QueryProxy
func NewQueryProxy(wal *gwal.Log) (*QueryProxy, error) {
	inbox := make(chan *gactor.Query)
	return &QueryProxy{
		inbox:  inbox,
		pool:   NewWorkPool(wal, inbox),
		tables: gskl.New[[]byte, gactor.Actor](bytes.Compare),
	}, nil
}

func (qp *QueryProxy) Send(ctx context.Context, query *gactor.Query) {
	qp.pool.Send(ctx, query)
}

func StartProxy(ctx context.Context, qp *QueryProxy) {
	glog.Track("starting query proxy")
	qp.pool.Start(ctx)
	for _, table := range []string{"default", "a", "b", "c"} {
		query, done := gactor.NewAddTableQuery(
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
