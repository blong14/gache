package file_test

import (
	"context"
	"testing"

	gactors "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/actors/file"
)

func TestNew(t *testing.T) {
	actor := gfile.New()
	ctx, cancel := context.WithCancel(context.Background())
	go actor.Start(ctx)
	t.Cleanup(func() {
		actor.Stop(ctx)
		cancel()
	})

	query, done := gactors.NewLoadFromFileQuery([]byte("default"), []byte("i.json"))
	go actor.Execute(ctx, query)

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
			case <-actor.OnResult():
			}
		}
	}(ctx)

	result := <-done
	if !result.Success {
		t.Error("data not loaded")
	}
}
