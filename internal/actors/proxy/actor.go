package proxy

import (
	"bytes"
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	gtrace "github.com/blong14/gache/internal"
	gactor "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/actors/file/reader"
	gview "github.com/blong14/gache/internal/actors/view"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
	glog "github.com/blong14/gache/internal/logging"
	gpool "github.com/blong14/gache/internal/pool"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	log    *gwal.Log
	tracer trace.Tracer
	pool   *gpool.WorkPool
	// table name to table actor
	tables gcache.Table[[]byte, gactor.Actor]
	batch  int
}

var _ gactor.Actor = &QueryProxy{}

// NewQueryProxy returns a fully ready to use *QueryProxy
func NewQueryProxy(wal *gwal.Log) (*QueryProxy, error) {
	return &QueryProxy{
		log:    wal,
		tracer: otel.Tracer("query-proxy"),
		tables: gcache.New[[]byte, gactor.Actor](
			bytes.Compare, bytes.Equal),
	}, nil
}

func (qp *QueryProxy) SetPool(pool *gpool.WorkPool) *QueryProxy {
	qp.pool = pool
	return qp
}

func (qp *QueryProxy) Enqueue(ctx context.Context, query *gactor.Query) {
	qp.pool.Send(ctx, query)
}

func (qp *QueryProxy) Execute(ctx context.Context, query *gactor.Query) {
	ctx, span := gtrace.Trace(ctx, qp.tracer, query, "query-proxy")
	defer span.End()
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
		qp.tables.Set(query.Header.TableName, t)
		qp.log.Execute(ctx, query)
		query.Done(gactor.QueryResponse{Success: true})
	case gactor.GetValue, gactor.Print, gactor.Range,
		gactor.BatchSetValue, gactor.SetValue:
		table, ok := qp.tables.Get(query.Header.TableName)
		if !ok {
			return
		}
		table.Execute(ctx, query)
	case gactor.Load:
		glog.Track(
			"loading csv %s for %s", query.Header.FileName, query.Header.TableName)
		loader := gfile.New(qp.pool)
		loader.Execute(ctx, query)
	default:
	}
}

func StartProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("starting query proxy")
	pool := gpool.New(qp)
	pool.Start(ctx)
	qp.SetPool(pool)
	for _, table := range []string{"default", "a", "b", "c"} {
		query, done := gactor.NewAddTableQuery(
			ctx, []byte(table),
		)
		qp.Enqueue(ctx, query)
		<-done
	}
	log.Println("default tables added")
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stopping query proxy")
	qp.pool.WaitAndStop(ctx)
}
