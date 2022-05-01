package actors

import (
	"context"

	gcache "github.com/blong14/gache/internal/cache"
)

type Actor interface {
	Start(c context.Context)
	Stop(c context.Context)
}

type ViewActor interface {
	Actor
	Get(ctx context.Context, query *Query)
}

type viewActor struct {
	impl  *gcache.DB
	inbox chan *Query
}

func (va *viewActor) Get(ctx context.Context, query *Query) {
	select {
	case <-ctx.Done():
	case va.inbox <- query:
	}
}

func (va *viewActor) Start(c context.Context) {
	for {
		select {
		case <-c.Done():
			return
		case query, ok := <-va.inbox:
			if !ok {
				return
			}
			query.OnResult(
				context.TODO(),
				Response{
					Key:     []byte("byte"),
					Value:   []byte("value"),
					Success: true,
				},
			)
		}
	}
}
func (va *viewActor) Stop(c context.Context) {
	close(va.inbox)
}

func NewViewActor() ViewActor {
	return &viewActor{
		inbox: make(chan *Query),
	}
}

type IndexActor interface {
	ViewActor
}

type WriteActor interface {
	Set(ctx context.Context, q *Query)
}
