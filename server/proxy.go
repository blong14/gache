package server

import (
	"bytes"
	"context"

	gmap "gache/cache/kv/sorted"
)

type response struct {
	Value   []byte
	Success bool
}

type message struct {
	Key   []byte
	Value []byte
	done  chan response
}

func (m *message) finish() {
	close(m.done)
}

// result blocks and waits for a response on the message's done channel
// and returns a slice of bytes and the message's status
func (m *message) result(ctx context.Context) ([]byte, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case resp := <-m.done:
		return resp.Value, resp.Success
	}
}

var (
	inbox  = make(chan message)
	outbox = make(chan message)
)

type QueryProxy struct {
	cache *gmap.TableMap[[]byte, []byte]
}

func NewQueryProxy() (*QueryProxy, error) {
	cache := gmap.New[[]byte, []byte](bytes.Compare)
	return &QueryProxy{cache: cache}, nil
}

func (qp *QueryProxy) Listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-outbox:
			value, ok := qp.cache.Get(msg.Key)
			select {
			case <-ctx.Done():
				msg.finish()
				return
			case msg.done <- response{Value: value, Success: ok}:
			}
		case msg := <-inbox:
			qp.cache.Set(msg.Key, msg.Value)
			select {
			case <-ctx.Done():
				msg.finish()
				return
			case msg.done <- response{Value: msg.Value, Success: true}:
			}
		}
	}
}

func (qp *QueryProxy) Get(ctx context.Context, key []byte) ([]byte, bool) {
	msg := message{
		Key:  key,
		done: make(chan response),
	}
	defer msg.finish()
	select {
	case <-ctx.Done():
	case outbox <- msg:
	}
	value, ok := msg.result(ctx)
	return value, ok
}

func (qp *QueryProxy) Set(ctx context.Context, key, value []byte) bool {
	msg := message{
		Key:   key,
		Value: value,
		done:  make(chan response),
	}
	defer msg.finish()
	select {
	case <-ctx.Done():
	case inbox <- msg:
	}
	_, ok := msg.result(ctx)
	return ok
}

func (qp *QueryProxy) Range(f func(k, v any) bool) {
	qp.cache.Range(f)
}

func CloseQueryProxy(_ *QueryProxy) {
	close(inbox)
	close(outbox)
}
