package file

import (
	"encoding/json"
	"io/ioutil"

	gerrors "github.com/blong14/gache/internal/errors"
)

func ReadJSON(data string) ([]KeyValue, error) {
	js, err := ioutil.ReadFile(data)
	if err != nil {
		return nil, gerrors.NewGError(err)
	}
	var out []KeyValue
	if err := json.Unmarshal(js, &out); err != nil {
		return nil, gerrors.NewGError(err)
	}
	return out, nil
}
