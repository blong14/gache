package reader_test

import (
	"context"
	"testing"

	gactors "github.com/blong14/gache/internal/actors"
	greader "github.com/blong14/gache/internal/actors/file/reader"
)

func TestNew(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	actor := greader.New()
	go actor.Init(ctx)
	t.Cleanup(func() {
		cancel()
	})

	query, done := gactors.NewLoadFromFileQuery([]byte("default"), []byte("i.csv"))
	go actor.Execute(ctx, query)

	finished := make(chan struct{})
	go func(ctx context.Context) {
		defer close(finished)
		for {
			select {
			case <-ctx.Done():
			case queries, ok := <-actor.OnResult():
				if !ok {
					return
				}
				for _, query := range queries {
					query.Finish(ctx)
				}
			}
		}
	}(ctx)

	result := <-done
	if !result.Success {
		t.Error("data not loaded")
	}

	<-finished
}
