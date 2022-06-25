package proxy

import (
	"bytes"
	"context"
	genv "github.com/blong14/gache/internal/environment"
	glog "github.com/blong14/gache/internal/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log"

	gactor "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/actors/file/reader"
	gview "github.com/blong14/gache/internal/actors/view"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	log    *gwal.Log
	tracer trace.Tracer
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
		tables: gcache.XNew[[]byte, gactor.Actor](
			bytes.Compare, bytes.Equal),
	}, nil
}

func (qp *QueryProxy) Execute(ctx context.Context, query *gactor.Query) {
	var span trace.Span
	if genv.TraceEnabled() {
		ctx, span = qp.tracer.Start(
			ctx, "query-proxy:Execute")
		defer span.End()
		span.SetAttributes(
			attribute.String(
				"query-instruction",
				query.Header.Inst.String(),
			),
		)
	}
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
		table, ok := qp.tables.Get(query.Header.TableName)
		if !ok {
			return
		}
		loader := gfile.New(table)
		loader.Execute(ctx, query)
	default:
	}
}

func StartProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("starting query proxy")
	for _, table := range []string{"default", "a", "b", "c"} {
		query, done := gactor.NewAddTableQuery(
			ctx, []byte(table),
		)
		qp.Execute(ctx, query)
		<-done
		close(done)
	}
	log.Println("default tables added")
}
