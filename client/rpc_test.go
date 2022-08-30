package client_test

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/blong14/gache/client"
)

func MustGetDB() *sql.DB {
	dsn := "::memory::"
	db, err := sql.Open("gache", dsn)
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}
	return db
}

func TestClient(t *testing.T) {
	ctx := context.Background()
	c := client.New(nil, MustGetDB())
	table, key, value := []byte("default"), []byte("__key__"), []byte("__value__")
	if err := c.Set(ctx, table, key, value); err != nil {
		t.Error(err)
	}
	actual, err := c.Get(ctx, []byte("default"), []byte("__key__"))
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(actual, value) {
		t.Error("value not found")
	}
	_, err = c.Get(ctx, []byte("default"), []byte("__not_found__"))
	if err == nil {
		t.Error("should not have found the key")
	}
}
