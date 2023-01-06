package db_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gdb "github.com/blong14/gache/internal/db"
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
	// t.Skip("skipping...")
	t.Cleanup(func() {
		tearDown(t)
	})
	start := time.Now()
	opts := &gdb.TableOpts{
		DataDir:   []byte("testdata"),
		TableName: []byte("default"),
		InMemory:  false,
		WalMode:   true,
	}
	db := gdb.New(opts)

	// given
	var wg sync.WaitGroup
	count := 50_000
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := db.Set(
				[]byte(fmt.Sprintf("key_%d", idx)), []byte(fmt.Sprintf("value__%d", idx)))
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	// when
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if _, ok := db.Get([]byte(fmt.Sprintf("key_%d", idx))); !ok {
				t.Errorf("missing rawKey %d", idx)
			}
		}(i)
	}
	wg.Wait()

	t.Logf("%s", time.Since(start))

	start = time.Now()
	values, ok := db.Scan([]byte("key_45"), []byte("key_48"))
	if !ok {
		t.Errorf("missing keys %v", values)
	}
	if len(values) == 0 {
		t.Errorf("missing keys %v", values)
	}

	t.Logf("%s %+v", time.Since(start), values)

	db.Close()
}

func TestInMemoryDB(t *testing.T) {
	start := time.Now()
	opts := &gdb.TableOpts{
		DataDir:   []byte("testdata"),
		TableName: []byte("default"),
		InMemory:  true,
		WalMode:   false,
	}
	db := gdb.New(opts)

	// given
	var wg sync.WaitGroup
	count := 50_000
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := db.Set(
				[]byte(fmt.Sprintf("key_%d", idx)), []byte(fmt.Sprintf("value__%d", idx)))
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	// when
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if _, ok := db.Get([]byte(fmt.Sprintf("key_%d", idx))); !ok {
				t.Errorf("missing rawKey %d", idx)
			}
		}(i)
	}
	wg.Wait()

	t.Logf("%s", time.Since(start))
	db.Close()

}
