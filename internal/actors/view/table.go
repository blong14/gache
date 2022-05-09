package view

import (
	"context"

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
	defer glog.Track("%T %s stopped", va, va.name)
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
				go func() {
					defer query.Finish(ctx)
					query.OnResult(ctx, resp)
				}()
			case gactors.Print:
				va.impl.Print()
				go func() {
					defer query.Finish(ctx)
					var resp gactors.QueryResponse
					resp.Success = true
					query.OnResult(ctx, resp)
				}()
			case gactors.SetValue:
				va.impl.Set(query.Key, query.Value)
				go func() {
					defer query.Finish(ctx)
					var resp gactors.QueryResponse
					resp.Key = query.Key
					resp.Value = query.Value
					resp.Success = true
					query.OnResult(ctx, resp)
				}()
			default:
				query.Finish(ctx)
			}
		}
	}
}

func (va *tableImpl) Close(_ context.Context) {
	glog.Track("%T %s stopping...", va, va.name)
	close(va.done)
	close(va.inbox)
}

func (va *tableImpl) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-ctx.Done():
	case <-va.done:
	case va.inbox <- query:
	}
}
