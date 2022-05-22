package replication

import (
	"context"
	"net/rpc"

	gactors "github.com/blong14/gache/internal/actors"
	gerrors "github.com/blong14/gache/internal/errors"
	glog "github.com/blong14/gache/internal/logging"
	gproxy "github.com/blong14/gache/internal/proxy"
)

// implements Actor interface
type queryReplicator struct {
	inbox  chan *gactors.Query
	done   chan struct{}
	client *rpc.Client
	errs   *gerrors.Error
}

func New(client *rpc.Client) gactors.Actor {
	return &queryReplicator{
		client: client,
		inbox:  make(chan *gactors.Query),
		done:   make(chan struct{}),
	}
}

func (r *queryReplicator) Init(ctx context.Context) {
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
			case gactors.AddTable, gactors.SetValue:
				r.errs = gerrors.Append(
					r.errs,
					gerrors.OnlyError(gproxy.PublishQuery(r.client, query)),
				)
			default:
			}
		}
	}
}

func (r *queryReplicator) Close(_ context.Context) {
	close(r.done)
}

func (r *queryReplicator) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-r.done:
	case <-ctx.Done():
	case r.inbox <- query:
	}
}
