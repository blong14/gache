package json_test

import (
	"context"
	"testing"

	gjson "github.com/blong14/gache/internal/io/json"
)

func TestReadJSON(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done, err := gjson.ReadJSON(ctx, "i.json")
	if err != nil {
		t.Error(err)
	}
	value := <-done
	cancel()
	if value == nil {
		t.Error("value is nil")
	}
}
