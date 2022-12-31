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
		InMemory:  true,
	}
	db := gdb.New(opts)

	// given
	count := 150
	done := make(chan struct{})
	go func() {
		start := glog.Trace("set", time.Time{})
		for i := 0; i < count; i++ {
			err := db.Set(
				[]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value__%d", i)))
			if err != nil {
				t.Error(err)
			}
		}
		glog.Trace("set", start)
		close(done)
	}()

	// when
	<-done
	start := glog.Trace("get", time.Time{})
	for i := 0; i < count; i++ {
		if _, ok := db.Get([]byte(fmt.Sprintf("key_%d", i))); !ok {
			t.Errorf("missing rawKey %d", i)
		}
	}
	glog.Trace("get", start)
	db.Close()
}
