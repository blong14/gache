package db_test

import (
	"encoding/binary"
	"math/rand"
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

func setUp(t *testing.T, count int) [][]byte {
	keys := make([][]byte, 0)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < count; i++ {
		buf := make([]byte, 8)
		key := random(rng, buf)
		keys = append(keys, key)
	}
	return keys
}

func random(rng *rand.Rand, b []byte) []byte {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

func TestFileDB(t *testing.T) {
	//t.Skip("skipping...")
	t.Cleanup(func() {
		//tearDown(t)
	})
	db := gdb.New(
		&gdb.TableOpts{
			DataDir:   []byte("testdata"),
			TableName: []byte("default"),
			InMemory:  false,
			WalMode:   true,
		},
	)
	count := 64
	keys := setUp(t, count)
	// given
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func(k []byte) {
			defer wg.Done()
			err := db.Set(k, k)
			if err != nil {
				t.Error(err)
			}
		}(key)
	}
	wg.Wait()
	// when
	for _, i := range keys {
		wg.Add(1)
		go func(k []byte) {
			defer wg.Done()
			if _, ok := db.Get(k); !ok {
				t.Errorf("missing rawKey %s", k)
			}
		}(i)
	}
	wg.Wait()
	db.Close()
}

func TestInMemoryDB(t *testing.T) {
	db := gdb.New(
		&gdb.TableOpts{
			DataDir:   []byte("testdata"),
			TableName: []byte("default"),
			InMemory:  true,
			WalMode:   false,
		},
	)
	count := 64
	keys := setUp(t, count)
	// given
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func(k []byte) {
			defer wg.Done()
			err := db.Set(k, k)
			if err != nil {
				t.Error(err)
			}
		}(key)
	}
	wg.Wait()
	// when
	for _, i := range keys {
		wg.Add(1)
		go func(k []byte) {
			defer wg.Done()
			if _, ok := db.Get(k); !ok {
				t.Errorf("missing rawKey %s", k)
			}
		}(i)
	}
	wg.Wait()
	db.Close()
}
