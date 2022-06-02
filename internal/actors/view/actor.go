package view

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	gactors "github.com/blong14/gache/internal/actors"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
	genv "github.com/blong14/gache/internal/environment"
	glog "github.com/blong14/gache/internal/logging"
)

// Table implements Actor
type Table struct {
	log    *gwal.Log
	tracer trace.Tracer
	inbox  chan *gactors.Query
	done   chan struct{}
	impl   gcache.Table
	name   []byte
}

func New(wal *gwal.Log, opts *gcache.TableOpts) gactors.Actor {
	return &Table{
		name:   opts.TableName,
		tracer: otel.Tracer("table-proxy"),
		log:    wal,
		impl:   gcache.NewSkipListDB(opts),
		inbox:  make(chan *gactors.Query),
		done:   make(chan struct{}),
	}
}

func (va *Table) Init(parentCtx context.Context) {
	glog.Track("%T %s waiting for work", va, va.name)
	for {
		select {
		case <-parentCtx.Done():
			return
		case <-va.done:
			return
		case query, ok := <-va.inbox:
			if !ok {
				return
			}
			queryCtx := query.Context()
			var span trace.Span
			if genv.TraceEnabled() {
				queryCtx, span = va.tracer.Start(
					queryCtx, "table-proxy:proxy",
				)
			}
			va.log.Execute(parentCtx, query)
			switch query.Header.Inst {
			case gactors.GetValue:
				go func(ctx context.Context, q *gactors.Query) {
					var resp gactors.QueryResponse
					if value, ok := va.impl.Get(q.Key); ok {
						resp = gactors.QueryResponse{
							Key:     q.Key,
							Value:   value,
							Success: true,
						}
					}
					q.Done(resp)
				}(queryCtx, query)
			case gactors.Print:
				go func(ctx context.Context, q *gactors.Query) {
					va.impl.Print()
					q.Done(gactors.QueryResponse{Success: true})
				}(queryCtx, query)
			case gactors.Range:
				go func(ctx context.Context, q *gactors.Query) {
					va.impl.Range(func(k, v any) bool {
						select {
						case <-ctx.Done():
							return false
						default:
						}
						fmt.Printf("%s ", k)
						return true
					})
					q.Done(gactors.QueryResponse{Success: true})
				}(queryCtx, query)
			case gactors.SetValue:
				go func(ctx context.Context, q *gactors.Query) {
					var childSpan trace.Span
					if genv.TraceEnabled() {
						ctx, childSpan = va.tracer.Start(
							ctx, "table-proxy:SetValue",
						)
						childSpan.SetAttributes(
							attribute.Int("query-length", len(q.Values)),
							attribute.String("query-instruction", q.Header.Inst.String()),
						)
					}
					va.impl.(gcache.TableTracer).TraceSet(ctx, q.Key, q.Value)
					if genv.TraceEnabled() {
						childSpan.End()
					}
					q.Done(gactors.QueryResponse{Success: true, Key: q.Key, Value: q.Value})
				}(queryCtx, query)
			case gactors.BatchSetValue:
				go func(ctx context.Context, q *gactors.Query) {
					fmt.Printf("batch set value %d\n", len(q.Values))
					var childSpan trace.Span
					if genv.TraceEnabled() {
						ctx, childSpan = va.tracer.Start(
							ctx, "table-proxy:BatchSetValue",
						)
						childSpan.SetAttributes(
							attribute.Int("query-length", len(q.Values)),
							attribute.String("query-instruction", q.Header.Inst.String()),
						)
					}
					for _, kv := range q.Values {
						if kv.Valid() {
							va.impl.(gcache.TableTracer).TraceSet(ctx, kv.Key, kv.Value)
						}
					}
					if genv.TraceEnabled() {
						childSpan.End()
					}
					q.Done(gactors.QueryResponse{Success: true})
				}(queryCtx, query)
			default:
			}
			if genv.TraceEnabled() {
				span.End()
			}
		}
	}
}

func (va *Table) Close(_ context.Context) {
	close(va.done)
}

func (va *Table) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-va.done:
	case <-ctx.Done():
	case va.inbox <- query:
	}
}
