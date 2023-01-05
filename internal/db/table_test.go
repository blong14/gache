package db_test

import (
	"fmt"
	"testing"
	"time"

	gdb "github.com/blong14/gache/internal/db"
	glog "github.com/blong14/gache/internal/logging"
)

func TestGetAndSet(t *testing.T) {
	opts := &gdb.TableOpts{
		DataDir:   []byte("testdata"),
		TableName: []byte("default"),
		InMemory:  false,
		WalMode:   true,
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

func TestScanAndSet(t *testing.T) {
	opts := &gdb.TableOpts{
		DataDir:   []byte("testdata"),
		TableName: []byte("default"),
		InMemory:  true,
	}
	db := gdb.New(opts)

	// given
	count := 10
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
	start := glog.Trace("get", time.Time{})
	for i := 0; i < count; i++ {
		values, ok := db.Scan([]byte(fmt.Sprintf("key_%d", i)), nil)
		if !ok {
			t.Errorf("missing rawKey %d", i)
		}
		t.Log(len(values))
	}
	glog.Trace("get", start)
	db.Close()
}
