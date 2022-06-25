package pool

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/internal/logging"
)

type XActor interface {
	Send(ctx context.Context, query *gactors.Query) bool
	Start(ctx context.Context)
	Stop(ctx context.Context)
}

type Worker struct {
	id      string
	healthz chan struct{}
	inbox   chan *gactors.Query
	stop    chan interface{}
	proxy   gactors.Actor
}

func (s *Worker) Start(ctx context.Context) {
	glog.Track("%s starting", s.id)
	for {
		select {
		case <-ctx.Done():
			glog.Track("%s ctx canceled", s.id)
			return
		case <-s.stop:
			glog.Track("%s stopping", s.id)
			return
		case query := <-s.inbox:
			glog.Track(
				"%s executing %s values=%d",
				s.id, query.Header.Inst, len(query.Values),
			)
			s.proxy.Execute(query.Context(), query)
		}
	}
}

func (s *Worker) Send(ctx context.Context, query *gactors.Query) bool {
	select {
	case <-ctx.Done():
		return false
	case s.inbox <- query:
		return true
	default:
		return false
	}
}

func (s *Worker) Stop(ctx context.Context) {
	select {
	case <-ctx.Done():
	case s.stop <- struct{}{}:
	}
}

type WorkPool struct {
	proxy   gactors.Actor
	healthz chan struct{}
	workers []Worker
}

func New(proxy gactors.Actor) *WorkPool {
	return &WorkPool{
		proxy:   proxy,
		healthz: make(chan struct{}, 1),
		workers: make([]Worker, 0),
	}
}

var (
	inbox = make(chan *gactors.Query, 1)
)

func (s *WorkPool) Start(ctx context.Context) {
	for i := 0; i < runtime.NumCPU(); i++ {
		worker := Worker{
			id:      fmt.Sprintf("worker::%d", i),
			proxy:   s.proxy,
			inbox:   inbox,
			stop:    make(chan interface{}, 0),
			healthz: s.healthz,
		}
		s.workers = append(s.workers, worker)
		go worker.Start(ctx)
	}
}

func (s *WorkPool) Send(ctx context.Context, query *gactors.Query) {
	select {
	case <-ctx.Done():
	case inbox <- query:
	default:
	}
}

func (s *WorkPool) Wait(ctx context.Context) {
	var wg sync.WaitGroup
	for _, worker := range s.workers {
		wg.Add(1)
		go func(w Worker) {
			defer wg.Done()
			w.Stop(ctx)
			close(w.stop)
		}(worker)
	}
	wg.Wait()
}
