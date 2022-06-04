package cache

type KV[K, V any] interface {
	Get(k K) (V, bool)
	Range(func(k K, v V) bool)
	Remove(k K) (V, bool)
	Set(k K, v V)
}
