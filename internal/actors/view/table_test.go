package view_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gview "github.com/blong14/gache/internal/actors/view"
	gcache "github.com/blong14/gache/internal/cache"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	glog "github.com/blong14/gache/logging"
)

func assertMatch(t *testing.T, want []byte, got []byte) {
	if !(bytes.Compare(want, got) == 0) {
		t.Errorf("want %s got %s", want, got)
	}
}

func testGet_Hit(v gactors.Actor, expected *gactors.QueryResponse) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		query, outbox := gactors.NewGetValueQuery([]byte("default"), expected.Key)
		go v.Execute(context.TODO(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual := <-outbox:
			if !actual.Success {
				t.Errorf("not ok %v", query)
			}
			assertMatch(t, expected.Value, actual.Value)
		}
	}
}

func TestViewActor_Get(t *testing.T) {
	t.Parallel()
	// given
	ctx := context.TODO()
	opts := &gcache.TableOpts{
		WithCache: func() *gtree.TreeMap[[]byte, []byte] {
			start := time.Now()
			impl := gtree.New[[]byte, []byte](bytes.Compare)
			impl.Set([]byte("key"), []byte("value"))
			glog.Track("startup=%s", time.Since(start))
			return impl
		},
	}
	v := gview.New(opts)
	go v.Start(ctx)
	hit := &gactors.QueryResponse{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	t.Run("hit", testGet_Hit(v, hit))
	t.Cleanup(func() {
		v.Stop(ctx)
	})
}
