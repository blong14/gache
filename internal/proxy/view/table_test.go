package view_test

import (
	"bytes"
	"context"
	"testing"

	gdb "github.com/blong14/gache/internal/db"
	gview "github.com/blong14/gache/internal/proxy/view"
)

func assertMatch(t *testing.T, want []byte, got []byte) {
	if !bytes.Equal(want, got) {
		t.Errorf("want %s got %s", want, got)
	}
}

func testGet_Hit(ctx context.Context, v *gview.Table, expected *gdb.QueryResponse) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		query, outbox := gdb.NewSetValueQuery(ctx,
			[]byte("default"), expected.Key, expected.Value)
		go v.Execute(query.Context(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual, ok := <-outbox:
			if !ok || !actual.Success {
				t.Errorf("not ok %v", query)
			}
		}

		query, outbox = gdb.NewGetValueQuery(ctx, []byte("default"), expected.Key)
		query.KeyRange.Start = expected.Key
		go v.Execute(query.Context(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual, ok := <-outbox:
			if !ok || !actual.Success {
				t.Errorf("not ok %v", query)
			}
			for _, r := range actual.RangeValues {
				assertMatch(t, expected.Value, r[1])
			}
		}
	}
}

func testScan_Hit(ctx context.Context, v *gview.Table, expected *gdb.QueryResponse) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		query, outbox := gdb.NewSetValueQuery(ctx,
			[]byte("default"), expected.Key, expected.Value)
		go v.Execute(query.Context(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual, ok := <-outbox:
			if !ok || !actual.Success {
				t.Errorf("not ok %v", query)
			}
		}

		query, outbox = gdb.NewGetValueQuery(ctx, []byte("default"), expected.Key)
		query.Header.Inst = gdb.GetRange
		query.KeyRange.Start = expected.Key
		go v.Execute(query.Context(), query)
		select {
		case <-ctx.Done():
			t.Error(ctx.Err())
		case actual, ok := <-outbox:
			if !ok || !actual.Success {
				t.Errorf("not ok %v", query)
			}
			for _, r := range actual.RangeValues {
				assertMatch(t, expected.Value, r[1])
			}
		}
	}
}
func TestViewActor_Get(t *testing.T) {
	t.Parallel()
	// given
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	opts := &gdb.TableOpts{
		TableName: []byte("default"),
		InMemory:  true,
	}
	v := gview.New(opts)
	hit := &gdb.QueryResponse{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	t.Run("hit", testGet_Hit(ctx, v, hit))
	t.Run("hit", testScan_Hit(ctx, v, hit))
}
