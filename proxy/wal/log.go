package wal

import (
	"container/list"
	"context"

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
				w.impl.PushBack(query)
				for _, sub := range w.subscriptions {
					go sub.Execute(ctx, query)
				}
			}
		}
	}
}

func (w *WAL) Stop(ctx context.Context) {
	glog.Track("%T stopping...", w)
	for _, sub := range w.subscriptions {
		sub.Close(ctx)
	}
	close(w.done)
	close(w.inbox)
	glog.Track("%T stopped", w)
}

func (w *WAL) Execute(ctx context.Context, entries ...*gactors.Query) {
	if w.done == nil || w.inbox == nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-w.done:
	case w.inbox <- entries:
	}
}
