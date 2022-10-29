package cache_test

import (
	"bytes"
	"testing"

	gache "github.com/blong14/gache/internal/cache"
)

func TestReader_ViewGet(t *testing.T) {
	t.Parallel()
	// given
	k := []byte("key")
	expected := []byte("value")
	v := gache.New()
	v.Set(k, expected)
	v.Set([]byte("key2"), []byte("value2"))
	// when
	actual, ok := v.Get(k)
	// then
	if !ok || !bytes.Equal(actual, expected) {
		t.Errorf("want %s got %s", expected, actual)
	}
}
