package proxy

import (
	"bytes"
	"context"
	"log"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	gactor "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/actors/file/reader"
	gview "github.com/blong14/gache/internal/actors/view"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	genv "github.com/blong14/gache/internal/environment"
	glog "github.com/blong14/gache/internal/logging"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	inbox  chan *gactor.Query
	done   chan struct{}
	log    *gwal.Log
	tracer trace.Tracer
	// table name to table actor
	tables *gtree.TreeMap[[]byte, gactor.Actor]
}

var _ gactor.Actor = &QueryProxy{}

// NewQueryProxy returns a fully ready to use *QueryProxy
func NewQueryProxy(wal *gwal.Log) (*QueryProxy, error) {
	return &QueryProxy{
		log:    wal,
		tracer: otel.Tracer("query-proxy"),
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
			qp.log.Close(ctx)
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
				return
			}
			queryCtx := query.Context()
			var span trace.Span
			if genv.TraceEnabled() {
				queryCtx, span = qp.tracer.Start(
					queryCtx, "query-proxy:proxy",
				)
				span.SetAttributes(
					attribute.String("query-instruction", query.Header.Inst.String()),
				)
			}
			qp.log.Execute(parentCtx, query)
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
				t := gview.New(qp.log, opts)
				go t.Init(parentCtx)
				qp.tables.Set(query.Header.TableName, t)
				query.Done(gactor.QueryResponse{Success: true})
			case gactor.GetValue, gactor.Print, gactor.Range, gactor.BatchSetValue, gactor.SetValue:
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					continue
				}
				table.Execute(queryCtx, query)
			case gactor.Load:
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					continue
				}
				loader := gfile.New(table)
				go loader.Init(parentCtx)
				loader.Execute(queryCtx, query)
			default:
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
		go qp.log.Init(ctx)
		go qp.Init(ctx)
		query, done := gactor.NewAddTableQuery(ctx, []byte("default"))
		qp.Execute(ctx, query)
		<-done
		log.Println("default table added")
		close(done)

		query, done = gactor.NewAddTableQuery(ctx, []byte("skiplist"))
		query.Header.Opts = &gcache.TableOpts{
			WithSkipList: func() *gskl.SkipList[[]byte, []byte] {
				return gskl.New[[]byte, []byte](bytes.Compare, bytes.Equal)
			},
		}
		qp.Execute(ctx, query)
		<-done
		log.Println("skiplist table added")
		close(done)

		query, done = gactor.NewAddTableQuery(ctx, []byte("treemap"))
		query.Header.Opts = &gcache.TableOpts{
			WithTreeMap: func() *gtree.TreeMap[[]byte, []byte] {
				return gtree.New[[]byte, []byte](bytes.Compare)
			},
		}
		qp.Execute(ctx, query)
		<-done
		log.Println("treemap table added")
		close(done)
	})
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stopping query proxy")
	qp.Close(ctx)
}
