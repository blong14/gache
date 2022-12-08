package db_test

import (
	"bytes"
	"testing"

	gdb "github.com/blong14/gache/internal/db"
)

func TestReader_ViewGet(t *testing.T) {
	key := []byte("name")
	expected := []byte("__value__")
	opts := &gdb.TableOpts{
		DataDir:   []byte("testdata"),
		TableName: []byte("default"),
		InMemory:  false,
	}

	db := gdb.New(opts)
	defer db.Close()

	// given
	err := db.Set(key, expected)
	if err != nil {
		t.Error(err)
	}
	err = db.Set([]byte("__test__"), []byte("__value__"))
	if err != nil {
		t.Error(err)
	}

	// when
	actual, ok := db.Get(key)

	// then
	if !ok || !bytes.Equal(actual, expected) {
		t.Errorf("want %s got %s", expected, actual)
	}
}
