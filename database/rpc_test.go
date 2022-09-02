package database_test

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	gache "github.com/blong14/gache/database"
	"github.com/blong14/gache/internal/actors/proxy"
)

func MustGetDB() *sql.DB {
	db, err := sql.Open("gache", gache.MEMORY)
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
	db := MustGetDB()
	c := gache.New(nil, db)
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
	qp, _ := gache.GetProxy(db)
	proxy.StopProxy(ctx, qp)
}
