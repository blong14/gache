package client_test

import (
	"bytes"
	"context"
	"testing"

	gclient "github.com/blong14/gache/client"
)

func TestClient(t *testing.T) {
	ctx := context.Background()
	conn := gclient.NewProxyClient()
	table, key, value := []byte("default"), []byte("__key__"), []byte("__value__")
	if err := conn.Set(ctx, table, key, value); err != nil {
		t.Error(err)
	}
	actual, err := conn.Get(ctx, table, key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(actual, value) {
		t.Error("value not found")
	}
	_, err = conn.Get(ctx, table, []byte("__not_found__"))
	if err == nil {
		t.Error("should not have found the key")
	}
	err = conn.Close(ctx)
	if err != nil {
		t.Error(err)
	}
}

