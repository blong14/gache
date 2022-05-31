package metrics

import (
	"context"

	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/internal/logging"
)

// Collector implements Actor interface
type Collector struct {
	inbox chan *gactors.Query
	done  chan struct{}
}

func New() gactors.Actor {
	return &Collector{
		inbox: make(chan *gactors.Query),
		done:  make(chan struct{}),
	}
}

func (m *Collector) Init(ctx context.Context) {
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

func (m *Collector) Close(_ context.Context) {
	close(m.done)
}

func (m *Collector) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-m.done:
	case <-ctx.Done():
	case m.inbox <- query:
	}
}
