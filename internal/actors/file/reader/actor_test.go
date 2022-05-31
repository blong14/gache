package reader_test

import (
	"context"
	"testing"

	gactors "github.com/blong14/gache/internal/actors"
	greader "github.com/blong14/gache/internal/actors/file/reader"
	gview "github.com/blong14/gache/internal/actors/view"
	gcache "github.com/blong14/gache/internal/cache"
)

func TestNew(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	table := gview.New(
		&gcache.TableOpts{
			TableName: []byte("default"),
		},
	)
	go table.Init(ctx)

	actor := greader.New(table)
	go actor.Init(ctx)
	t.Cleanup(func() {
		cancel()
	})

	query, done := gactors.NewLoadFromFileQuery(ctx, []byte("default"), []byte("i.csv"))
	defer close(done)
	go actor.Execute(ctx, query)

	result := <-done
	if !result.GetResponse().Success {
		t.Error("data not loaded")
	}
}
