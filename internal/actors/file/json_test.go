package file_test

import (
	"context"
	"testing"

	gactors "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/actors/file"
)

func TestNew(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	actor := gfile.New()
	go actor.Init(ctx)
	t.Cleanup(func() {
		actor.Close(ctx)
		cancel()
	})

	query, done := gactors.NewLoadFromFileQuery([]byte("default"), []byte("i.json"))
	go actor.Execute(ctx, query)

	finished := make(chan struct{})
	go func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case queries := <-actor.OnResult():
			for _, query := range queries {
				var resp gactors.QueryResponse
				resp.Key = query.Key
				resp.Value = query.Value
				resp.Success = true
				query.OnResult(ctx, resp)
				query.Finish(ctx)
			}
			close(finished)
		}
	}(ctx)

	result := <-done
	if !result.Success {
		t.Error("data not loaded")
	}

	<-finished
}
