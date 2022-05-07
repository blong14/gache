package replication

import (
	"context"
	gerrors "github.com/blong14/gache/errors"
	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/logging"
	gproxy "github.com/blong14/gache/proxy"
	"net/rpc"
	"time"
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

func (r *queryReplicator) Start(ctx context.Context) {
	glog.Track("%T waiting for work", r)
	defer glog.Track("%T stopped", r)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	queries := make([]*gactors.Query, 0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-ticker.C:
			if !(len(queries) > 0) {
				continue
			}
			r.errs = gerrors.Append(
				r.errs,
				gerrors.OnlyError(gproxy.PublishQuery(r.client, queries...)),
			)
			queries = []*gactors.Query{}
		case query, ok := <-r.inbox:
			if !ok {
				return
			}
			if r.client == nil {
				continue
			}
			queries = append(queries, query)
		}
	}
}

func (r *queryReplicator) Stop(_ context.Context) {
	glog.Track("%T stopping...", r)
	close(r.inbox)
	close(r.done)
}

func (r *queryReplicator) Execute(ctx context.Context, query *gactors.Query) {
	select {
	case <-ctx.Done():
	case <-r.done:
	case r.inbox <- query:
	}
}
