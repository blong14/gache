package actors

import (
	"context"
)

type Actor interface {
	Start(c context.Context)
	Stop(c context.Context)
	Execute(ctx context.Context, query *Query)
}

type Streamer interface {
	Actor
	OnResult() <-chan []*Query
}
