package metrics

import (
	"context"

	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/internal/logging"
)

// implements Actor interface
type collector struct {
	inbox chan *gactors.Query
	done  chan struct{}
}

func New() gactors.Actor {
	return &collector{
		inbox: make(chan *gactors.Query),
		done:  make(chan struct{}),
	}
}

func (m *collector) Init(ctx context.Context) {
	glog.Track("%T waiting for work", m)
	defer glog.Track("%T stopped", m)
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.done:
			return
		case _, ok := <-m.inbox:
			if !ok {
				return
			}
			continue
			// glog.Track("%s", query)
		}
	}
}

func (m *collector) Close(_ context.Context) {
	close(m.done)
}

func (m *collector) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-m.done:
	case <-ctx.Done():
	case m.inbox <- query:
	}
}
