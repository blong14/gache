package view_test

import (
	"bytes"
	"context"
	gactors "github.com/blong14/gache/internal/actors"
	gview "github.com/blong14/gache/internal/actors/view"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
	"testing"
)

func assertMatch(t *testing.T, want []byte, got []byte) {
	if !bytes.Equal(want, got) {
		t.Errorf("want %s got %s", want, got)
	}
}

func testGet_Hit(ctx context.Context, v gactors.Actor, expected *gactors.QueryResponse) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		query, outbox := gactors.NewGetValueQuery(ctx, []byte("default"), expected.Key)
		defer close(outbox)
		v.Execute(query.Context(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual, ok := <-outbox:
			if !ok || !actual.GetResponse().Success {
				t.Errorf("not ok %v", query)
			}
			assertMatch(t, expected.Value, actual.GetResponse().Value)
		}
	}
}

func TestViewActor_Get(t *testing.T) {
	t.Parallel()
	// given
	ctx, cancel := context.WithCancel(context.Background())
	opts := &gcache.TableOpts{
		WithSkipList: func() *gskl.SkipList[[]byte, []byte] {
			impl := gskl.New[[]byte, []byte](bytes.Compare, bytes.Equal)
			impl.Set([]byte("key"), []byte("value"))
			return impl
		},
	}
	wal := gwal.New()
	go wal.Init(ctx)
	v := gview.New(wal, opts)
	go v.Init(ctx)
	hit := &gactors.QueryResponse{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	t.Run("hit", testGet_Hit(ctx, v, hit))
	t.Cleanup(func() {
		cancel()
		v.Close(ctx)
		wal.Close(ctx)
	})
}
