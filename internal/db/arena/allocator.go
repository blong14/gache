package arena

import (
	"context"
	"sync"

	ga "arena"
)

var mtx sync.Mutex

var ballast = 4096

type ByteArena []byte

func (na *ByteArena) Allocate(len_ int) []byte {
	mtx.Lock()
	defer mtx.Unlock()
	if len(*na) == 0 {
		*na = make([]byte, ballast)
		ballast *= 2
	}
	offset := (len(*na) - 1) - len_
	if offset <= 0 {
		*na = make([]byte, len(*na)+len_)
		offset = (len(*na) - 1) - len_
	}
	n := (*na)[offset : len(*na)-1]
	*na = (*na)[:offset]
	return n
}

type Arena interface {
	AllocateByteSlice(len_, cap int) []byte
	Free()
}

type arena struct {
	malloc *ga.Arena
}

func NewArena() Arena {
	return &arena{malloc: ga.NewArena()}
}

func (a *arena) Free() {
	a.malloc.Free()
}

func (a *arena) AllocateByteSlice(len_, cap int) []byte {
	return ga.MakeSlice[byte](a.malloc, len_, cap)
}

type Message struct {
	Status string
}

func NewMessage() *Message {
	return &Message{}
}

type Worker interface {
	Context() context.Context
	Start() error
	Stop() error
}

var mailbox = make(chan *Message)

type worker struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mailbox chan *Message
	outbox  chan *Message
}

func NewWorker(ctx context.Context, cancel context.CancelFunc) Worker {
	return &worker{
		ctx:     ctx,
		cancel:  cancel,
		mailbox: mailbox,
		outbox:  make(chan *Message),
	}
}

func (w *worker) Context() context.Context {
	if w.ctx == nil {
		w.ctx = context.Background()
		return w.ctx
	}
	return w.ctx
}

func Send(ctx context.Context, msg *Message) <-chan *Message {
	out := make(chan *Message)
	select {
	case <-ctx.Done():
	case mailbox <- msg:
		go func() {
			defer close(out)
			select {
			case <-ctx.Done():
			case out <- <-msg.outbox:
			}
		}()
	}
	return out
}

func (w *worker) Start() error {
	for range w.mailbox {
		select {
		case <-w.Context().Done():
			return w.Context().Err()
		case w.outbox <- &Message{Status: "ok"}:
		}
	}
	return nil
}

func (w *worker) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}
	close(w.mailbox)
	close(w.outbox)
	return nil
}
