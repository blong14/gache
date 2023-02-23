package db

import "encoding/json"

type KeyValue struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

func (kv *KeyValue) Valid() bool {
	return len(kv.Key) > 0
}
