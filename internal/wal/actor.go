package wal

import (
	"container/list"
	"context"

	"go.opentelemetry.io/otel"

	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/internal/logging"
)

// WAL implements gactors.Actor
type WAL struct {
	inbox         chan []*gactors.Query
	done          chan struct{}
	impl          *list.List
	subscriptions []gactors.Actor
}

func New(subs ...gactors.Actor) *WAL {
	return &WAL{
		impl:          list.New(),
		inbox:         make(chan []*gactors.Query),
		done:          make(chan struct{}),
		subscriptions: subs,
	}
}

func (w *WAL) Start(ctx context.Context) {
	glog.Track("%T waiting for work", w)
	for _, sub := range w.subscriptions {
		go sub.Init(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			for _, sub := range w.subscriptions {
				sub.Close(ctx)
			}
			return
		case queries, ok := <-w.inbox:
			if !ok {
				return
			}
			for _, query := range queries {
				spanCtx, span := otel.Tracer("query-wal").Start(
					query.Context(), "query-wal:proxy",
				)
				w.impl.PushBack(query)
				for _, sub := range w.subscriptions {
					go sub.Execute(spanCtx, query)
				}
				span.End()
			}
		}
	}
}

func (w *WAL) Stop(_ context.Context) {
	close(w.done)
}

func (w *WAL) Execute(ctx context.Context, entries ...*gactors.Query) {
	select {
	case <-w.done:
	case <-ctx.Done():
	case w.inbox <- entries:
	}
}
