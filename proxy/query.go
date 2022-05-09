package proxy

import (
	"bytes"
	"context"
	gfile "github.com/blong14/gache/internal/actors/file"
	"log"
	"sync"
	"time"

	gactor "github.com/blong14/gache/internal/actors"
	gview "github.com/blong14/gache/internal/actors/view"
	gcache "github.com/blong14/gache/internal/cache"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	glog "github.com/blong14/gache/logging"
	gwal "github.com/blong14/gache/proxy/wal"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	inbox chan *gactor.Query
	done  chan struct{}
	log   *gwal.WAL
	// table name to table actor
	tables *gtree.TreeMap[[]byte, gactor.Actor]
}

// NewQueryProxy returns a fully ready to use *QueryProxy
func NewQueryProxy(wal *gwal.WAL) (*QueryProxy, error) {
	return &QueryProxy{
		log:    wal,
		inbox:  make(chan *gactor.Query),
		done:   make(chan struct{}),
		tables: gtree.New[[]byte, gactor.Actor](bytes.Compare),
	}, nil
}

func (qp *QueryProxy) Init(parentCtx context.Context) {
	glog.Track("%T waiting for work", qp)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case <-qp.done:
			return
		case query, ok := <-qp.inbox:
			if !ok {
				if query != nil {
					query.Finish(ctx)
				}
				return
			}
			go qp.log.Execute(ctx, query)
			switch query.Header.Inst {
			case gactor.AddTable:
				table := gview.New(
					&gcache.TableOpts{
						TableName: query.Header.TableName,
					},
				)
				go table.Init(ctx)
				qp.tables.Set(query.Header.TableName, table)
				go func() {
					defer query.Finish(ctx)
					var result gactor.QueryResponse
					result.Success = true
					query.OnResult(ctx, result)
				}()
			case gactor.GetValue, gactor.Print, gactor.SetValue:
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					query.Finish(ctx)
					continue
				}
				go table.Execute(ctx, query)
			case gactor.Load:
				loader := gfile.New()
				glog.Track("%T start %T", qp, loader)
				go loader.Init(ctx)
				go loader.Execute(ctx, query)
				go func() {
					start := time.Now()
					for queries := range loader.(gactor.Streamer).OnResult() {
						for _, query := range queries {
							go qp.Execute(ctx, query)
						}
					}
					glog.Track("queries=%s", time.Since(start))
				}()
			default:
				query.Finish(ctx)
			}
		}
	}
}

func (qp *QueryProxy) Close(ctx context.Context) {
	glog.Track("%T stopping...", qp)
	close(qp.inbox)
	close(qp.done)
	qp.log.Stop(ctx)
	qp.tables.Range(func(_, v any) bool {
		sub, ok := v.(gactor.Actor)
		if !ok {
			return true
		}
		sub.Close(ctx)
		return true
	})
	glog.Track("%T stopped", qp)
}

func (qp *QueryProxy) Execute(ctx context.Context, query *gactor.Query) {
	select {
	case <-ctx.Done():
	case <-qp.done:
	case qp.inbox <- query:
	}
}

var onc sync.Once

func StartProxy(ctx context.Context, qp *QueryProxy) {
	onc.Do(func() {
		log.Println("starting query proxy")
		go qp.log.Start(ctx)
		go qp.Init(ctx)
		query, done := gactor.NewAddTableQuery([]byte("default"))
		qp.Execute(ctx, query)
		<-done
	})
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stopping query proxy")
	qp.Close(ctx)
}
