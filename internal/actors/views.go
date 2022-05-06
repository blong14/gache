package actors

import (
	"context"
	gcache "github.com/blong14/gache/internal/cache"
	glog "github.com/blong14/gache/logging"
)

type Actor interface {
	Start(c context.Context)
	Stop(c context.Context)
	Execute(ctx context.Context, query *Query)
}

// implements Actor
type tableImpl struct {
	inbox chan *Query
	impl  gcache.Table
	name  []byte
}

func NewTableActor(opts *gcache.TableOpts) Actor {
	return &tableImpl{
		name:  opts.TableName,
		impl:  gcache.NewTable(opts),
		inbox: make(chan *Query),
	}
}

func (va *tableImpl) Start(ctx context.Context) {
	glog.Track("%T %s waiting for work", va, va.name)
	shouldContinue := true
	for {
		if !shouldContinue {
			break
		}
		select {
		case <-ctx.Done():
			shouldContinue = false
			break
		case query, ok := <-va.inbox:
			if !ok {
				shouldContinue = false
				break
			}
			switch query.Header.Inst {
			case GetValue:
				var resp QueryResponse
				if value, ok := va.impl.Get(query.Key); ok {
					resp = QueryResponse{
						Key:     query.Key,
						Value:   value,
						Success: true,
					}
				}
				go func() {
					defer query.Finish()
					query.OnResult(ctx, resp)
				}()
			case SetValue:
				va.impl.Set(query.Key, query.Value)
				go func() {
					defer query.Finish()
					query.OnResult(
						ctx,
						QueryResponse{
							Key:     query.Key,
							Value:   query.Value,
							Success: true,
						},
					)
				}()
			}
		}
	}
	glog.Track("%T %s stopped", va, va.name)
}

func (va *tableImpl) Execute(ctx context.Context, query *Query) {
	select {
	case <-ctx.Done():
	case va.inbox <- query:
	}
}

func (va *tableImpl) Stop(_ context.Context) {
	glog.Track("%T %s stopping...", va, va.name)
	close(va.inbox)
}

// implements Actor interface
type metrics struct {
	inbox chan *Query
	done  chan struct{}
}

func NewMetricsSubscriber() Actor {
	return &metrics{
		inbox: make(chan *Query),
		done:  make(chan struct{}),
	}
}

func (m *metrics) Start(ctx context.Context) {
	glog.Track("%T waiting for work", m)
	defer glog.Track("%T stopped", m)
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.done:
			return
		case query, ok := <-m.inbox:
			if !ok {
				return
			}
			glog.Track("%s", query)
		}
	}
}

func (m *metrics) Stop(_ context.Context) {
	glog.Track("%T stopping...", m)
	close(m.inbox)
	close(m.done)
}

func (m *metrics) Execute(ctx context.Context, query *Query) {
	select {
	case <-ctx.Done():
	case <-m.done:
	case m.inbox <- query:
	}
}
