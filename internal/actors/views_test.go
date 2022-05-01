package actors_test

import (
	"bytes"
	"context"
	"testing"

	gactors "github.com/blong14/gache/internal/actors"
)

func assertMatch(t *testing.T, want []byte, got []byte) {
	if !(bytes.Compare(want, got) == 0) {
		t.Errorf("want %s got %s", want, got)
	}
}

func testGet_Hit(v gactors.TableActor, expected *gactors.QueryResponse) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		ctx := context.TODO()
		query := gactors.NewGetValueQuery([]byte("default"), expected.Key)
		go v.Get(context.TODO(), query)
		value, ok := query.Result(ctx)
		if !ok {
			t.Errorf("not ok %v", query)
		}
		assertMatch(t, expected.Value, value)
	}
}

func TestViewActor_Get(t *testing.T) {
	t.Parallel()
	// given
	ctx := context.TODO()
	v := gactors.NewTableActor()
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
