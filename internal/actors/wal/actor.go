package wal

import (
	"context"

	gactors "github.com/blong14/gache/internal/actors"
	gcache "github.com/blong14/gache/internal/cache"
)

// Log implements gactors.Actor
type Log struct {
	impl          *gcache.TableCache
	subscriptions []gactors.Actor
}

// Log implements Actor/Streamer interfaces
var _ gactors.Actor = &Log{}

func New(subs ...gactors.Actor) *Log {
	return &Log{
		impl:          gcache.New(),
		subscriptions: subs,
	}
}

func (w *Log) Send(ctx context.Context, query *gactors.Query) {
	w.impl.Set(query.Key, query.Value)
	for _, sub := range w.subscriptions {
		go sub.Send(ctx, query)
	}
}
