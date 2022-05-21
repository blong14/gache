package skiplist_test

import (
	"bytes"
	"testing"

	glist "github.com/blong14/gache/internal/cache/sorted/skiplist"
)

func TestGetAndSet(t *testing.T) {
	t.Parallel()
	// given
	list := glist.New[[]byte, []byte](bytes.Compare)
	expected := "value"
	keys := []string{
		"key8",
		"key2",
		"key1",
		"key5",
		"key3",
		"key9",
		"key7",
		"key4",
		"key6",
	}

	done := make(chan struct{})
	go func() {
		// when
		for _, key := range keys {
			list.Set([]byte(key), []byte(expected))
		}
		done <- struct{}{}
	}()

	<-done
	for _, key := range keys {
		if value, ok := list.Get([]byte(key)); !ok {
			t.Errorf("missing key %s %s", key, value)
		}
	}
	list.Print()
	_, ok := list.Get([]byte("missing"))
	if ok {
		t.Error("key should be missing")
	}
}
