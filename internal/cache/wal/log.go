package wal

import "container/list"

type WAL struct {
	impl *list.List
}

func New() *WAL {
	return &WAL{
		impl: list.New(),
	}
}

func (w *WAL) Write(entries ...any) error {
	for _, e := range entries {
		w.impl.PushBack(e)
	}
	return nil
}
