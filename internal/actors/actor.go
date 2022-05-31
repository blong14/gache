package actors

import (
	"context"
)

type Actor interface {
	Init(c context.Context)
	Close(c context.Context)
	Execute(ctx context.Context, query *Query)
}

type Streamer interface {
	Actor
	ExecuteMany(ctx context.Context, query ...*Query)
}
