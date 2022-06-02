package wal

import (
	"container/list"
	"context"

	"go.opentelemetry.io/otel"

	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/internal/logging"
)

// Log implements gactors.Actor
type Log struct {
	inbox         chan *gactors.Query
	batch         chan []*gactors.Query
	done          chan struct{}
	impl          *list.List
	subscriptions []gactors.Actor
}

// Log implements Actor/Streamer interfaces
var (
	_ gactors.Actor    = &Log{}
	_ gactors.Streamer = &Log{}
)

func New(subs ...gactors.Actor) *Log {
	return &Log{
		impl:          list.New(),
		done:          make(chan struct{}),
		batch:         make(chan []*gactors.Query),
		inbox:         make(chan *gactors.Query),
		subscriptions: subs,
	}
}

func (w *Log) Init(parentCtx context.Context) {
	glog.Track("%T waiting for work", w)
	for _, sub := range w.subscriptions {
		go sub.Init(parentCtx)
	}
	for {
		select {
		case <-parentCtx.Done():
			return
		case <-w.done:
			for _, sub := range w.subscriptions {
				sub.Close(parentCtx)
			}
			return
		case query, ok := <-w.inbox:
			if !ok {
				return
			}
			w.impl.PushBack(query)
			for _, sub := range w.subscriptions {
				go sub.Execute(parentCtx, query)
			}
		case queries, ok := <-w.batch:
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

func (w *Log) Close(_ context.Context) {
	close(w.done)
}

func (w *Log) Execute(ctx context.Context, entry *gactors.Query) {
	select {
	case <-w.done:
	case <-ctx.Done():
	case w.inbox <- entry:
	}
}

func (w *Log) ExecuteMany(ctx context.Context, entries ...*gactors.Query) {
	select {
	case <-w.done:
	case <-ctx.Done():
	case w.batch <- entries:
	}
}
