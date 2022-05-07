package json

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	gerrors "github.com/blong14/gache/errors"
)

type KeyValue struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

func ReadJSON(ctx context.Context, data string) (<-chan []*KeyValue, error) {
	f, err := os.Open(data)
	if err != nil {
		return nil, gerrors.NewGError(err)
	}
	out := make(chan []*KeyValue)
	go func() {
		defer close(out)
		defer func() { _ = f.Close() }()
		reader := json.NewDecoder(f)
		var data []*KeyValue
		if err := reader.Decode(&data); err != nil {
			log.Fatal(fmt.Errorf("read error %w", err))
		}
		select {
		case <-ctx.Done():
		case out <- data:
		}
	}()
	return out, nil
}
