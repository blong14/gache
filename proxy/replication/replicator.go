package replication

import (
	"context"
	"log"
	"net/rpc"
	"time"

	"go.opentelemetry.io/otel"

	gerrors "github.com/blong14/gache/errors"
	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/logging"
	gproxy "github.com/blong14/gache/proxy"
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
			// actor:instruction:indentifier
			_, span := otel.Tracer("").Start(query.Context(), "query-repl:gactors.Append:Query")
			switch query.Header.Inst {
			case gactors.AddTable, gactors.SetValue:
				start := time.Now()
				r.errs = gerrors.Append(
					r.errs,
					gerrors.OnlyError(gproxy.PublishQuery(r.client, query)),
				)
				log.Printf("%s\n", time.Since(start))
			default:
			}
			span.End()
		}
	}
}

func (r *queryReplicator) Close(ctx context.Context) {
	_, span := otel.Tracer("").Start(ctx, "query-repl.Close")
	defer span.End()
	close(r.inbox)
	close(r.done)
	span.End()
}

func (r *queryReplicator) Execute(ctx context.Context, query *gactors.Query) {
	spanCtx, span := otel.Tracer("").Start(ctx, "query-repl.Execute")
	defer span.End()
	select {
	case <-spanCtx.Done():
	case <-r.done:
	case r.inbox <- query:
	}
}
