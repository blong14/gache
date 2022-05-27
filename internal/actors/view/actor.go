package view

import (
	"context"

	"go.opentelemetry.io/otel"

	gactors "github.com/blong14/gache/internal/actors"
	gcache "github.com/blong14/gache/internal/cache"
	glog "github.com/blong14/gache/internal/logging"
)

// implements Actor
type tableImpl struct {
	concurrent bool
	inbox      chan *gactors.Query
	done       chan struct{}
	impl       gcache.Table
	name       []byte
}

func New(opts *gcache.TableOpts) gactors.Actor {
	return &tableImpl{
		concurrent: opts.Concurrent,
		name:       opts.TableName,
		impl:       gcache.NewTable(opts),
		inbox:      make(chan *gactors.Query),
		done:       make(chan struct{}),
	}
}

func (va *tableImpl) Init(ctx context.Context) {
	glog.Track("%T %s waiting for work", va, va.name)
	for {
		select {
		case <-ctx.Done():
			return
		case <-va.done:
			return
		case query, ok := <-va.inbox:
			if !ok {
				if query != nil {
					query.Finish(ctx)
				}
				return
			}
			spanCtx, span := otel.Tracer("query-view").Start(
				query.Context(), "query-view:proxy")
			switch query.Header.Inst {
			case gactors.GetValue:
				get := func(ctx context.Context) {
					defer query.Finish(spanCtx)
					var resp gactors.QueryResponse
					if value, ok := va.impl.Get(query.Key); ok {
						resp = gactors.QueryResponse{
							Key:     query.Key,
							Value:   value,
							Success: true,
						}
					}
					query.OnResult(spanCtx, resp)
				}
				if va.concurrent {
					go get(query.Context())
				} else {
					get(query.Context())
				}
			case gactors.Print:
				go func(ctx context.Context) {
					defer query.Finish(spanCtx)
					va.impl.Print()
					var resp gactors.QueryResponse
					resp.Success = true
					query.OnResult(spanCtx, resp)
				}(query.Context())
			case gactors.Range:
				go func(ctx context.Context, q *gactors.Query) {
					defer q.Finish(ctx)
					va.impl.Range(ctx)
					var resp gactors.QueryResponse
					resp.Success = true
					query.OnResult(ctx, resp)
				}(spanCtx, query)
			case gactors.SetValue:
				go func(q *gactors.Query) {
					ctx := q.Context()
					defer q.Finish(ctx)
					va.impl.TraceSet(ctx, query.Key, query.Value)
					var resp gactors.QueryResponse
					resp.Key = query.Key
					resp.Value = query.Value
					resp.Success = true
					q.OnResult(ctx, resp)
				}(query)
			default:
				query.Finish(ctx)
			}
			span.End()
		}
	}
}

func (va *tableImpl) Close(_ context.Context) {
	close(va.done)
}

func (va *tableImpl) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-va.done:
	case <-ctx.Done():
	case va.inbox <- query:
	}
}
