package replication

import (
	"context"
	"net/rpc"

	gactors "github.com/blong14/gache/internal/actors"
	gproxy "github.com/blong14/gache/internal/actors/proxy"
	glog "github.com/blong14/gache/internal/logging"
)

// QueryReplicator implements Actor interface
type QueryReplicator struct {
	client *rpc.Client
}

func New(client *rpc.Client) gactors.Actor {
	return &QueryReplicator{
		client: client,
	}
}

func (r *QueryReplicator) Execute(_ context.Context, query *gactors.Query) {
	switch query.Header.Inst {
	case gactors.AddTable, gactors.BatchSetValue, gactors.SetValue:
		if r.client != nil {
			_, err := gproxy.PublishQuery(r.client, query)
			if err != nil {
				glog.Track("%s", err)
			}
		}
	default:
	}
}
