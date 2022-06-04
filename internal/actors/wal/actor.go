package wal

import (
	"bytes"
	"context"

	gactors "github.com/blong14/gache/internal/actors"
	gcache "github.com/blong14/gache/internal/cache"
)

// Log implements gactors.Actor
type Log struct {
	impl          gcache.Table[[]byte, []byte]
	subscriptions []gactors.Actor
}

// Log implements Actor/Streamer interfaces
var _ gactors.Actor = &Log{}

func New(subs ...gactors.Actor) *Log {
	return &Log{
		impl:          gcache.XNew[[]byte, []byte](bytes.Compare, bytes.Equal),
		subscriptions: subs,
	}
}

func (w *Log) Execute(ctx context.Context, query *gactors.Query) {
	w.impl.Set(query.Key, query.Value)
	for _, sub := range w.subscriptions {
		go sub.Execute(ctx, query)
	}
}
