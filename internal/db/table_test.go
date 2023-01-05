package db_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	gdb "github.com/blong14/gache/internal/db"
	glog "github.com/blong14/gache/internal/logging"
)

func tearDown(t *testing.T) {
	err := os.Remove(filepath.Join("testdata", "default-wal.dat"))
	if err != nil {
		t.Log(err)
	}
	err = os.Remove(filepath.Join("testdata", "default.dat"))
	if err != nil {
		t.Log(err)
	}
}

func TestFileDB(t *testing.T) {
	t.Skip("skipping...")
	t.Cleanup(func() {
		tearDown(t)
	})
	db := gdb.New(
		&gdb.TableOpts{
			DataDir:   []byte("testdata"),
			TableName: []byte("default"),
			InMemory:  false,
			WalMode:   true,
		},
	)

	// given
	count := 50
	done := make(chan struct{})
	go func() {
		for i := 0; i < count; i++ {
			err := db.Set(
				[]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value__%d", i)))
			if err != nil {
				t.Error(err)
			}
		}
		close(done)
	}()

	// when
	<-done
	for i := 0; i < count; i++ {
		if _, ok := db.Get([]byte(fmt.Sprintf("key_%d", i))); !ok {
			t.Errorf("missing rawKey %d", i)
		}
	}

	values, ok := db.Scan([]byte("key_45"), []byte("key_48"))
	if !ok {
		t.Errorf("missing keys %v", values)
	}
	if len(values) == 0 {
		t.Errorf("missing keys %v", values)
	}

	db.Close()
}

func TestInMemoryDB(t *testing.T) {
	opts := &gdb.TableOpts{
		DataDir:   []byte("testdata"),
		TableName: []byte("default"),
		InMemory:  true,
		WalMode:   false,
	}
	db := gdb.New(opts)

	// given
	count := 50
	done := make(chan struct{})
	go func() {
		stop := glog.TraceStart("set")
		for i := 0; i < count; i++ {
			err := db.Set(
				[]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value__%d", i)))
			if err != nil {
				t.Error(err)
			}
		}
		stop()
		close(done)
	}()

	// when
	<-done
	for i := 0; i < count; i++ {
		if _, ok := db.Get([]byte(fmt.Sprintf("key_%d", i))); !ok {
			t.Errorf("missing rawKey %d", i)
		}
	}
	db.Close()
}
