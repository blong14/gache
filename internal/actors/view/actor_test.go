package view_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gview "github.com/blong14/gache/internal/actors/view"
	gcache "github.com/blong14/gache/internal/cache"
	gskl "github.com/blong14/gache/internal/cache/sorted/skiplist"
)

func assertMatch(t *testing.T, want []byte, got []byte) {
	if !bytes.Equal(want, got) {
		t.Errorf("want %s got %s", want, got)
	}
}

func testGet_Hit(v gactors.Actor, expected *gactors.QueryResponse) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		query, outbox := gactors.NewGetValueQuery(ctx, []byte("default"), expected.Key)
		defer close(outbox)
		go v.Execute(context.TODO(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual := <-outbox:
			if !actual.GetResponse().Success {
				t.Errorf("not ok %v", query)
			}
			assertMatch(t, expected.Value, actual.GetResponse().Value)
		}
	}
}

func TestViewActor_Get(t *testing.T) {
	t.Parallel()
	// given
	ctx := context.TODO()
	opts := &gcache.TableOpts{
		WithSkipList: func() *gskl.SkipList[[]byte, []byte] {
			impl := gskl.New[[]byte, []byte](bytes.Compare, bytes.Equal)
			impl.Set([]byte("key"), []byte("value"))
			return impl
		},
	}
	v := gview.New(opts)
	go v.Init(ctx)
	hit := &gactors.QueryResponse{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	t.Run("hit", testGet_Hit(v, hit))
	t.Cleanup(func() {
		v.Close(ctx)
	})
}
