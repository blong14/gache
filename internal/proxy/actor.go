package proxy

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	gactor "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/actors/file/reader"
	gview "github.com/blong14/gache/internal/actors/view"
	gcache "github.com/blong14/gache/internal/cache"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	genv "github.com/blong14/gache/internal/environment"
	grate "github.com/blong14/gache/internal/limiter"
	glog "github.com/blong14/gache/internal/logging"
	gwal "github.com/blong14/gache/internal/wal"
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
			qp.log.Stop(ctx)
			qp.tables.Range(func(_, v any) bool {
				sub, ok := v.(gactor.Actor)
				if !ok {
					return true
				}
				sub.Close(ctx)
				return true
			})
			return
		case query, ok := <-qp.inbox:
			if !ok {
				if query != nil {
					query.Finish(ctx)
				}
				return
			}
			ctx := query.Context()
			var span trace.Span
			if genv.TraceEnabled() {
				ctx, span = otel.Tracer("query-proxy").Start(
					ctx, "query-proxy:proxy",
				)
			}
			go qp.log.Execute(ctx, query)
			switch query.Header.Inst {
			case gactor.AddTable:
				table := gview.New(
					&gcache.TableOpts{
						Concurrent: true,
						TableName:  query.Header.TableName,
					},
				)
				go table.Init(ctx)
				qp.tables.Set(query.Header.TableName, table)
				go func(ctx context.Context) {
					defer query.Finish(ctx)
					var result gactor.QueryResponse
					result.Success = true
					query.OnResult(ctx, result)
				}(ctx)
			case gactor.GetValue, gactor.Print, gactor.Range, gactor.SetValue:
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					query.Finish(ctx)
					continue
				}
				go table.Execute(ctx, query)
			case gactor.Load:
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					query.Finish(ctx)
					continue
				}
				loader := gfile.New(table)
				go loader.Init(ctx)
				go loader.Execute(ctx, query)
			default:
				query.Finish(ctx)
			}
			if genv.TraceEnabled() {
				span.End()
			}
		}
	}
}

func (qp *QueryProxy) Close(_ context.Context) {
	close(qp.done)
}

func (qp *QueryProxy) Execute(ctx context.Context, query *gactor.Query) {
	select {
	case <-qp.done:
	case <-ctx.Done():
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
		log.Println("default table added")
	})
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stopping query proxy")
	qp.Close(ctx)
}
