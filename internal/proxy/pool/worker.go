package pool

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	gdb "github.com/blong14/gache/internal/db"
	garena "github.com/blong14/gache/internal/db/arena"
	glog "github.com/blong14/gache/internal/logging"
	gmap "github.com/blong14/gache/internal/map/tablemap"
	gfile "github.com/blong14/gache/internal/proxy/file"
)

type Worker interface {
	Start(ctx context.Context)
	Stop(ctx context.Context)
}

type worker struct {
	malloc garena.ByteArena
	id     string
	inbox  chan *gdb.Query
	stop   chan struct{}
	tables *gmap.TableMap[[]byte, *gdb.TableExecutor]
}

func (w *worker) Send(ctx context.Context, query *gdb.Query) {
	select {
	case <-ctx.Done():
	case w.inbox <- query:
	}
}

func (w *worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			glog.Track("%T::%s ctx canceled", w, w.id)
			return
		case <-w.stop:
			glog.Track("%T::%s stopping", w, w.id)
			return
		case query, ok := <-w.inbox:
			if !ok {
				return
			}
			start := time.Now()
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
				t := gdb.NewTable(opts)
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
				table.Execute(ctx, w.malloc, query)
			}
			glog.Track(
				"%T::%s executed %s %s [%s]",
				w, w.id, query.Header.Inst, query.Key, time.Since(start),
			)
		}
	}
}

func (w *worker) Stop(ctx context.Context) {}

type WorkPool struct {
	inbox   chan *gdb.Query
	workers []Worker
}

func New() *WorkPool {
	workPool := &WorkPool{
		inbox:   make(chan *gdb.Query),
		workers: make([]Worker, 0),
	}
	tables := gmap.New[[]byte, *gdb.TableExecutor](bytes.Compare)
	for i := 0; i < runtime.NumCPU(); i++ {
		w := &worker{
			id:     fmt.Sprintf("worker::%d", i),
			inbox:  workPool.inbox,
			stop:   make(chan struct{}),
			tables: tables,
		}
		workPool.workers = append(workPool.workers, w)
	}
	return workPool
}

func (p *WorkPool) Send(ctx context.Context, query *gdb.Query) {
	select {
	case <-ctx.Done():
	case p.inbox <- query:
	}
}

func (p *WorkPool) Start(ctx context.Context) {
	glog.Track("%T starting workers...\n", p)
	for _, w := range p.workers {
		go w.Start(ctx)
	}
}

func (p *WorkPool) Stop(_ context.Context) {
	close(p.inbox)
}

func WaitAndStop(ctx context.Context, pool *WorkPool) {
	glog.Track("%T stopping...\n", pool)
	var wg sync.WaitGroup
	for _, worker := range pool.workers {
		wg.Add(1)
		go func(w Worker) {
			defer wg.Done()
			w.Stop(ctx)
		}(worker)
	}
	wg.Wait()
	pool.Stop(ctx)
	glog.Track("%T stopped\n", pool)
}
