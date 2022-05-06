package cache_test

import (
	"bytes"
	"testing"

	gache "github.com/blong14/gache/internal/cache"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
)

func TestReader_ViewGet(t *testing.T) {
	// given
	k := []byte("key")
	expected := []byte("value")
	v := gache.NewTable(
		&gache.TableOpts{
			WithCache: func() *gtree.TreeMap[[]byte, []byte] {
				impl := gtree.New[[]byte, []byte](bytes.Compare)
				impl.Set(k, expected)
				return impl
			},
		},
	)

	// when
	actual, ok := v.Get(k)

	// then
	if !ok || !(bytes.Compare(actual, expected) == 0) {
		t.Errorf("want %s got %s", expected, actual)
	}
}