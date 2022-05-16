package wal

import (
	"container/list"
	"context"

	"go.opentelemetry.io/otel"

	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/logging"
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
			return
		case queries := <-w.inbox:
			for _, query := range queries {
				spanCtx, span := otel.Tracer("query-wal").Start(query.Context(), "query-wal:proxy")
				w.impl.PushBack(query)
				for _, sub := range w.subscriptions {
					go sub.Execute(spanCtx, query)
				}
				span.End()
			}
		}
	}
}

func (w *WAL) Stop(ctx context.Context) {
	spanCtx, span := otel.Tracer("").Start(ctx, "wal.Stop")
	defer span.End()
	for _, sub := range w.subscriptions {
		sub.Close(spanCtx)
	}
	close(w.done)
	close(w.inbox)
}

func (w *WAL) Execute(ctx context.Context, entries ...*gactors.Query) {
	spanCtx, span := otel.Tracer("").Start(ctx, "query-wal.Execute")
	defer span.End()
	if w.done == nil || w.inbox == nil {
		return
	}
	select {
	case <-spanCtx.Done():
	case <-w.done:
	case w.inbox <- entries:
	}
}
