package proxy

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"

	gactor "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/actors/file"
	gview "github.com/blong14/gache/internal/actors/view"
	gcache "github.com/blong14/gache/internal/cache"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	grate "github.com/blong14/gache/internal/limiter"
	glog "github.com/blong14/gache/logging"
	gwal "github.com/blong14/gache/proxy/wal"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	inbox   chan *gactor.Query
	done    chan struct{}
	log     *gwal.WAL
	limiter grate.RateLimiter
	// table name to table actor
	tables *gtree.TreeMap[[]byte, gactor.Actor]
}

// NewQueryProxy returns a fully ready to use *QueryProxy
func NewQueryProxy(wal *gwal.WAL) (*QueryProxy, error) {
	return &QueryProxy{
		log: wal,
		limiter: grate.MultiLimiter(
			rate.NewLimiter(
				grate.Per(1, time.Millisecond),
				grate.Burst(1),
			),
		),
		inbox:  make(chan *gactor.Query),
		done:   make(chan struct{}),
		tables: gtree.New[[]byte, gactor.Actor](bytes.Compare),
	}, nil
}

func (qp *QueryProxy) Init(parentCtx context.Context) {
	glog.Track("%T waiting for work", qp)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	defer func() { glog.Track("%T stopped", qp) }()
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
			spanCtx, span := otel.Tracer("query-proxy").Start(
				query.Context(), "query-proxy:proxy",
			)
			go qp.log.Execute(spanCtx, query)
			switch query.Header.Inst {
			case gactor.AddTable:
				table := gview.New(
					&gcache.TableOpts{
						TableName: query.Header.TableName,
					},
				)
				go table.Init(ctx)
				qp.tables.Set(query.Header.TableName, table)
				go func(ctx context.Context) {
					defer query.Finish(spanCtx)
					var result gactor.QueryResponse
					result.Success = true
					query.OnResult(spanCtx, result)
				}(spanCtx)
			case gactor.GetValue, gactor.Print, gactor.SetValue:
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					query.Finish(spanCtx)
					continue
				}
				go table.Execute(spanCtx, query)
			case gactor.Load:
				loader := gfile.New()
				go loader.Init(query.Context())
				go loader.Execute(query.Context(), query)
				go func(ctx context.Context) {
					for queries := range loader.(gactor.Streamer).OnResult() {
						for _, query := range queries {
							if err := qp.limiter.Wait(spanCtx); err != nil {
								return
							} else {
								qp.Execute(spanCtx, query)
							}
						}
					}
				}(query.Context())
			default:
				query.Finish(spanCtx)
			}
			span.End()
		}
	}
}

func (qp *QueryProxy) Close(ctx context.Context) {
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
		query, done := gactor.TraceNewAddTableQuery(ctx, []byte("default"))
		qp.Execute(ctx, query)
		<-done
	})
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stopping query proxy")
	qp.Close(ctx)
}
