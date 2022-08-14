package reader_test

import (
	"context"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	greader "github.com/blong14/gache/internal/actors/file/reader"
	gview "github.com/blong14/gache/internal/actors/view"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
	gpool "github.com/blong14/gache/internal/pool"
)

func TestNew(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	table := gview.New(gwal.New(), &gcache.TableOpts{TableName: []byte("default")})
	pool := gpool.New(table)
	pool.Start(ctx)
	t.Cleanup(func() { pool.WaitAndStop(ctx) })

	start := time.Now()
	query, done := gactors.NewLoadFromFileQuery(ctx, []byte("default"), []byte("i.csv"))
	actor := greader.New(pool)
	actor.Execute(ctx, query)
	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-done:
		if !ok || !result.Success {
			t.Error("data not loaded")
		}
	}
	t.Logf("finished in %s", time.Since(start))
}
