package reader_test

import (
	"context"
	"testing"

	gactors "github.com/blong14/gache/internal/actors"
	greader "github.com/blong14/gache/internal/actors/file/reader"
	gview "github.com/blong14/gache/internal/actors/view"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
)

func TestNew(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	wal := gwal.New()
	go wal.Init(ctx)
	table := gview.New(
		wal,
		&gcache.TableOpts{
			TableName: []byte("default"),
		},
	)
	go table.Init(ctx)

	actor := greader.New(table)
	go actor.Init(ctx)

	query, done := gactors.NewLoadFromFileQuery(ctx, []byte("default"), []byte("i.csv"))
	actor.Execute(ctx, query)
	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-done:
		if !ok || !result.GetResponse().Success {
			t.Error("data not loaded")
		}
	}
	t.Cleanup(func() {
		wal.Close(ctx)
		cancel()
	})
}
