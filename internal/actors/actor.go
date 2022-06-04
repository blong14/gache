package actors

import (
	"context"
)

type Actor interface {
	Execute(ctx context.Context, query *Query)
}
