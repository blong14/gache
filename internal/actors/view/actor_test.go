package view_test

import (
	"bytes"
	"context"
	"testing"

	gactors "github.com/blong14/gache/internal/actors"
	gview "github.com/blong14/gache/internal/actors/view"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
)

func assertMatch(t *testing.T, want []byte, got []byte) {
	if !bytes.Equal(want, got) {
		t.Errorf("want %s got %s", want, got)
	}
}

func testGet_Hit(ctx context.Context, v gactors.Actor, expected *gactors.QueryResponse) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		query, outbox := gactors.NewSetValueQuery(ctx,
			[]byte("default"), expected.Key, expected.Value)
		v.Execute(query.Context(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual, ok := <-outbox:
			if !ok || !actual.Success {
				t.Errorf("not ok %v", query)
			}
		}

		query, outbox = gactors.NewGetValueQuery(ctx, []byte("default"), expected.Key)
		v.Execute(query.Context(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual, ok := <-outbox:
			if !ok || !actual.Success {
				t.Errorf("not ok %v", query)
			}
			assertMatch(t, expected.Value, actual.Value)
		}
	}
}

func TestViewActor_Get(t *testing.T) {
	t.Parallel()
	// given
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	opts := &gcache.TableOpts{
		TableName: []byte("default"),
	}
	wal := gwal.New()
	v := gview.New(wal, opts)
	hit := &gactors.QueryResponse{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	t.Run("hit", testGet_Hit(ctx, v, hit))
}
