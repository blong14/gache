package replication

import (
	"context"
	"net/rpc"

	gactors "github.com/blong14/gache/internal/actors"
	gproxy "github.com/blong14/gache/internal/actors/proxy"
	gerrors "github.com/blong14/gache/internal/errors"
	glog "github.com/blong14/gache/internal/logging"
)

// QueryReplicator implements Actor interface
type QueryReplicator struct {
	inbox  chan *gactors.Query
	done   chan struct{}
	client *rpc.Client
	errs   *gerrors.Error
}

func New(client *rpc.Client) gactors.Actor {
	return &QueryReplicator{
		client: client,
		inbox:  make(chan *gactors.Query),
		done:   make(chan struct{}),
	}
}

func (r *QueryReplicator) Init(ctx context.Context) {
	glog.Track("%T waiting for work", r)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case query, ok := <-r.inbox:
			if !ok {
				return
			}
			if r.client == nil {
				continue
			}
			switch query.Header.Inst {
			case gactors.AddTable, gactors.BatchSetValue, gactors.SetValue:
				r.errs = gerrors.Append(
					r.errs,
					gerrors.OnlyError(gproxy.PublishQuery(r.client, query)),
				)
			default:
			}
		}
	}
}

func (r *QueryReplicator) Close(_ context.Context) {
	close(r.done)
}

func (r *QueryReplicator) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-r.done:
	case <-ctx.Done():
	case r.inbox <- query:
	}
}
