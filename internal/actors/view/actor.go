package view

import (
	"context"

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

func (va *tableImpl) Init(parentCtx context.Context) {
	glog.Track("%T %s waiting for work", va, va.name)
	for {
		select {
		case <-parentCtx.Done():
			return
		case <-va.done:
			return
		case query, ok := <-va.inbox:
			if !ok {
				if query != nil {
					query.Finish(parentCtx)
				}
				return
			}
			queryCtx := query.Context()
			switch query.Header.Inst {
			case gactors.GetValue:
				go func(ctx context.Context, q *gactors.Query) {
					defer q.Finish(ctx)
					var resp gactors.QueryResponse
					if value, ok := va.impl.Get(q.Key); ok {
						resp = gactors.QueryResponse{
							Key:     q.Key,
							Value:   value,
							Success: true,
						}
					}
					q.OnResult(ctx, resp)
				}(queryCtx, query)
			case gactors.Print:
				go func(ctx context.Context, q *gactors.Query) {
					defer q.Finish(ctx)
					va.impl.Print()
					q.OnResult(ctx, gactors.QueryResponse{Success: true})
				}(queryCtx, query)
			case gactors.Range:
				go func(ctx context.Context, q *gactors.Query) {
					defer q.Finish(ctx)
					va.impl.Range(ctx)
					q.OnResult(ctx, gactors.QueryResponse{Success: true})
				}(queryCtx, query)
			case gactors.SetValue:
				go func(ctx context.Context, q *gactors.Query) {
					defer q.Finish(ctx)
					va.impl.TraceSet(ctx, q.Key, q.Value)
					q.OnResult(ctx, gactors.QueryResponse{Success: true, Key: q.Key, Value: q.Value})
				}(queryCtx, query)
			case gactors.BatchSetValue:
				go func(ctx context.Context, q *gactors.Query) {
					defer q.Finish(ctx)
					for _, kv := range q.Values {
						if kv.Valid() {
							va.impl.TraceSet(ctx, kv.Key, kv.Value)
						}
					}
					q.OnResult(ctx, gactors.QueryResponse{Success: true})
				}(queryCtx, query)
			default:
				query.Finish(queryCtx)
			}
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
