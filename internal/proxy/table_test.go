package proxy_test

import (
	"bytes"
	"context"
	"testing"

	gdb "github.com/blong14/gache/internal/db"
	gproxy "github.com/blong14/gache/internal/proxy"
)

func assertMatch(t *testing.T, want []byte, got []byte) {
	if !bytes.Equal(want, got) {
		t.Errorf("want %s got %s", want, got)
	}
}

func testGetHit(ctx context.Context, v *gproxy.Table, expected *gdb.QueryResponse) func(t *testing.T) {
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

func testScanHit(ctx context.Context, v *gproxy.Table, expected *gdb.QueryResponse) func(t *testing.T) {
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

func TestTable_Get(t *testing.T) {
	t.Parallel()
	// given
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	opts := &gdb.TableOpts{
		TableName: []byte("default"),
		InMemory:  true,
	}
	v := gproxy.NewTable(opts)
	hit := &gdb.QueryResponse{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	t.Run("hit", testGetHit(ctx, v, hit))
	t.Run("hit", testScanHit(ctx, v, hit))
}
