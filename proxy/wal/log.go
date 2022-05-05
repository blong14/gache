package wal

import (
	"container/list"
	"context"

	gactors "github.com/blong14/gache/internal/actors"
)

// WAL implements gactors.Actor
type WAL struct {
	impl          *list.List
	inbox         chan []*gactors.Query
	subscriptions []gactors.Actor
}

func New(subs ...gactors.Actor) *WAL {
	return &WAL{
		impl:          list.New(),
		inbox:         make(chan []*gactors.Query),
		subscriptions: subs,
	}
}

func (w *WAL) Start(ctx context.Context) {
	for _, sub := range w.subscriptions {
		go sub.Start(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case queries := <-w.inbox:
			for _, query := range queries {
				w.impl.PushBack(query)
				w.onExecute(ctx, query)
			}
		}
	}
}

func (w *WAL) onExecute(ctx context.Context, query *gactors.Query) {
	for _, sub := range w.subscriptions {
		go sub.Execute(ctx, query)
	}
}

func (w *WAL) Stop(ctx context.Context) {
	for _, sub := range w.subscriptions {
		sub.Stop(ctx)
	}
	close(w.inbox)
}

func (w *WAL) Execute(ctx context.Context, entries ...*gactors.Query) {
	select {
	case <-ctx.Done():
	case w.inbox <- entries:
	}
}
