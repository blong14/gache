package file

import (
	"encoding/json"
	"os"

	gdb "github.com/blong14/gache/internal/db"
	gerrors "github.com/blong14/gache/internal/errors"
)

func ReadJSON(data string) ([]gdb.KeyValue, error) {
	js, err := os.ReadFile(data)
	if err != nil {
		return nil, gerrors.NewGError(err)
	}
	var out []gdb.KeyValue
	if err = json.Unmarshal(js, &out); err != nil {
		return nil, gerrors.NewGError(err)
	}
	return out, nil
}
