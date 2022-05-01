package cache

type KV interface {
	Get(k any) (any, bool)
	Range(func(k any, v any) bool)
	Remove(k any) (any, bool)
	Set(k any, v any)
}
