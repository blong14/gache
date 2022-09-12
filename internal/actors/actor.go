package actors

import (
	"context"
)

type Actor interface {
	Send(ctx context.Context, query *Query)
}
