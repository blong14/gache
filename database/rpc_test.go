package database_test

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	gdb "github.com/blong14/gache/database"
)

func MustGetDB() *sql.DB {
	db, err := sql.Open("gache", gdb.MEMORY)
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
	c := gdb.New(nil, db)
	table, key, value := []byte("default"), []byte("__key__"), []byte("__value__")
	if err := c.Set(ctx, table, key, value); err != nil {
		t.Error(err)
	}
	actual, err := c.Get(ctx, table, key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(actual, value) {
		t.Error("value not found")
	}
	_, err = c.Get(ctx, table, []byte("__not_found__"))
	if err == nil {
		t.Error("should not have found the key")
	}
	err = db.Close()
	if err != nil {
		t.Error(err)
	}
}
