package json

import (
	"encoding/json"
	"io/ioutil"

	gerrors "github.com/blong14/gache/errors"
)

type KeyValue struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

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
