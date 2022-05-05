package actors

import (
	"context"
	"sync"

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

func (va *tableImpl) Start(c context.Context) {
	glog.Track("%T %s waiting for work", va, va.name)
	for {
		select {
		case <-c.Done():
			return
		case query, ok := <-va.inbox:
			if !ok {
				return
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
					query.OnResult(c, resp)
				}()
			case SetValue:
				va.impl.Set(query.Key, query.Value)
				go func() {
					defer query.Finish()
					query.OnResult(
						c,
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
}

func (va *tableImpl) Execute(ctx context.Context, query *Query) {
	select {
	case <-ctx.Done():
	case va.inbox <- query:
	}
}

func (va *tableImpl) Stop(c context.Context) {
	close(va.inbox)
}

// implements Actor interface
type metrics struct {
	inbox chan *Query
}

func NewMetricsSubscriber() Actor {
	return &metrics{}
}

var once sync.Once

func (m *metrics) Start(ctx context.Context) {
	once.Do(func() {
		glog.Track("%T waiting for work", m)
		for {
			select {
			case <-ctx.Done():
				return
			case query := <-m.inbox:
				glog.Track("%s", query)
			}
		}
	})
}

func (m *metrics) Stop(_ context.Context) {
	close(m.inbox)
}

func (m *metrics) Execute(ctx context.Context, query *Query) {
	select {
	case <-ctx.Done():
	case m.inbox <- query:
	}
}
