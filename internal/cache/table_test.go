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
	v := gache.XNew[[]byte, []byte](bytes.Compare, bytes.Equal)
	v.Set(k, expected)
	// when
	actual, ok := v.Get(k)

	// then
	if !ok || !(bytes.Compare(actual, expected) == 0) {
		t.Errorf("want %s got %s", expected, actual)
	}
}
