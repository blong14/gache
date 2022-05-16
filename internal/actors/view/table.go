package view

import (
	"context"

	"go.opentelemetry.io/otel"

	gactors "github.com/blong14/gache/internal/actors"
	gcache "github.com/blong14/gache/internal/cache"
	glog "github.com/blong14/gache/logging"
)

// implements Actor
type tableImpl struct {
	inbox chan *gactors.Query
	done  chan struct{}
	impl  gcache.Table
	name  []byte
}

func New(opts *gcache.TableOpts) gactors.Actor {
	return &tableImpl{
		name:  opts.TableName,
		impl:  gcache.NewTable(opts),
		inbox: make(chan *gactors.Query),
		done:  make(chan struct{}),
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
			spanCtx, span := otel.Tracer("query-view").Start(query.Context(), "query-view:proxy")
			switch query.Header.Inst {
			case gactors.GetValue:
				var resp gactors.QueryResponse
				if value, ok := va.impl.Get(query.Key); ok {
					resp = gactors.QueryResponse{
						Key:     query.Key,
						Value:   value,
						Success: true,
					}
				}
				go func(ctx context.Context) {
					// actor:instruction:indentifier
					spanCtx, span := otel.Tracer("").Start(ctx, "query-view:gactors.GetValue:OnResult")
					defer query.Finish(spanCtx)
					defer span.End()
					query.OnResult(spanCtx, resp)
				}(query.Context())
			case gactors.Print:
				va.impl.Print()
				go func(ctx context.Context) {
					// actor:instruction:indentifier
					spanCtx, span := otel.Tracer("").Start(ctx, "query-view:gactors.Print:OnResult")
					defer query.Finish(spanCtx)
					defer span.End()
					var resp gactors.QueryResponse
					resp.Success = true
					query.OnResult(spanCtx, resp)
				}(query.Context())
			case gactors.SetValue:
				va.impl.TraceSet(spanCtx, query.Key, query.Value)
				go func(ctx context.Context) {
					// actor:instruction:indentifier
					spanCtx, span := otel.Tracer("query-view").Start(ctx, "query-view:gactors.SetValue:OnResult")
					defer query.Finish(spanCtx)
					defer span.End()
					var resp gactors.QueryResponse
					resp.Key = query.Key
					resp.Value = query.Value
					resp.Success = true
					query.OnResult(spanCtx, resp)
				}(spanCtx)
			default:
				query.Finish(ctx)
			}
			span.End()
		}
	}
}

func (va *tableImpl) Close(ctx context.Context) {
	_, span := otel.Tracer("").Start(ctx, "query-view:Close")
	defer span.End()
	if va.done == nil && va.inbox == nil {
		return
	}
	close(va.done)
	close(va.inbox)
}

func (va *tableImpl) Execute(ctx context.Context, query *gactors.Query) {
	spanCtx, span := otel.Tracer("").Start(ctx, "query-view:Execute")
	defer span.End()
	select {
	case <-spanCtx.Done():
	case <-va.done:
	case va.inbox <- query:
	}
}
