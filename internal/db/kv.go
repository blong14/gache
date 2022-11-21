package db

import "encoding/json"

type KV[K, V any] interface {
	Get(k K) (V, bool)
	Range(func(k K, v V) bool)
	Remove(k K) (V, bool)
	Set(k K, v V)
}

type KeyValue struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

func (kv *KeyValue) Valid() bool {
	return len(kv.Key) > 0
}
