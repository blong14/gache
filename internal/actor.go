package internal

import (
	"context"

	gdb "github.com/blong14/gache/internal/db"
)

type Actor interface {
	Send(ctx context.Context, q *gdb.Query)
}
