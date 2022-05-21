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
				go func(ctx context.Context) {
					var resp gactors.QueryResponse
					if value, ok := va.impl.Get(query.Key); ok {
						resp = gactors.QueryResponse{
							Key:     query.Key,
							Value:   value,
							Success: true,
						}
					}
					defer query.Finish(spanCtx)
					query.OnResult(spanCtx, resp)
				}(query.Context())
			case gactors.Print:
				go func(ctx context.Context) {
					va.impl.Print()
					defer query.Finish(spanCtx)
					var resp gactors.QueryResponse
					resp.Success = true
					query.OnResult(spanCtx, resp)
				}(query.Context())
			case gactors.SetValue:
				go func(ctx context.Context) {
					va.impl.TraceSet(spanCtx, query.Key, query.Value)
					defer query.Finish(spanCtx)
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

func (va *tableImpl) Close(_ context.Context) {
	if va.done == nil && va.inbox == nil {
		return
	}
	close(va.done)
	close(va.inbox)
}

func (va *tableImpl) Execute(ctx context.Context, query *gactors.Query) {
	if va.inbox == nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-va.done:
	case va.inbox <- query:
	}
}
