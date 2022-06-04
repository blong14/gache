package replication

import (
	"context"
	"log"
	"net/rpc"

	gactors "github.com/blong14/gache/internal/actors"
	gproxy "github.com/blong14/gache/internal/actors/proxy"
	gerrors "github.com/blong14/gache/internal/errors"
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
			if err := gerrors.OnlyError(gproxy.PublishQuery(r.client, query)); err != nil {
				log.Printf("%s\n", err)
			}
		}
	default:
	}
}
