package actors

import (
	"bytes"
	"context"
	gcache "github.com/blong14/gache/internal/cache"
	gtree "github.com/blong14/gache/internal/cache/sorted/tablemap"
	glog "github.com/blong14/gache/logging"
	"strconv"
	"time"
)

type Actor interface {
	Start(c context.Context)
	Stop(c context.Context)
}

type TableActor interface {
	Actor
	Get(ctx context.Context, query *Query)
	Set(ctx context.Context, query *Query)
}

type tableImpl struct {
	impl  gcache.Table
	inbox chan *Query
}

func (va *tableImpl) Get(ctx context.Context, query *Query) {
	select {
	case <-ctx.Done():
	case va.inbox <- query:
	}
}

func (va *tableImpl) Set(ctx context.Context, query *Query) {
	select {
	case <-ctx.Done():
	case va.inbox <- query:
	}
}

func (va *tableImpl) Start(c context.Context) {
	glog.Track("%T\tWaiting for work", va)
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
				glog.Track("%T Get key=%s", va.impl, string(query.Key))
				var resp QueryResponse
				if value, ok := va.impl.Get(query.Key); ok {
					resp = QueryResponse{
						Key:     query.Key,
						Value:   value,
						Success: true,
					}
				}
				query.OnResult(c, resp)
			case SetValue:
				va.impl.Set(query.Key, query.Value)
				query.OnResult(
					c,
					QueryResponse{
						Key:     query.Key,
						Value:   query.Value,
						Success: true,
					},
				)
			}
		}
	}
}

func (va *tableImpl) Stop(c context.Context) {
	close(va.inbox)
}

func NewTableActor() TableActor {
	return &tableImpl{
		impl: gcache.NewTable(
			&gcache.TableOpts{
				WithCache: func() *gtree.TableMap[[]byte, []byte] {
					start := time.Now()
					impl := gtree.New[[]byte, []byte](bytes.Compare)
					for i := 0; i < 1_000_000; i++ {
						impl.Set([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
					}
					glog.Track("startup=%s", time.Since(start))
					return impl
				},
			},
		),
		inbox: make(chan *Query),
	}
}
