package pool

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	glog "github.com/blong14/gache/internal/logging"
)

type Worker struct {
	id      string
	healthz chan struct{}
	inbox   chan *gactors.Query
	stop    chan interface{}
	proxy   gactors.Actor
}

func (s *Worker) Start(ctx context.Context) {
	glog.Track("%T::%s starting", s.proxy, s.id)
	for {
		select {
		case <-ctx.Done():
			glog.Track("%T::%s ctx canceled", s.proxy, s.id)
			return
		case <-s.stop:
			glog.Track("%T::%s stopping", s.proxy, s.id)
			return
		case query, ok := <-s.inbox:
			if !ok {
				return
			}
			start := time.Now()
			s.proxy.Execute(ctx, query)
			glog.Track(
				"%T::%s executed %s values=%d in %s",
				s.proxy, s.id, query.Header.Inst, len(query.Values), time.Since(start),
			)
		}
	}
}

func (s *Worker) Stop(ctx context.Context) {
	select {
	case <-ctx.Done():
	case s.stop <- struct{}{}:
	}
}

type WorkPool struct {
	inbox   chan *gactors.Query
	proxy   gactors.Actor
	healthz chan struct{}
	workers []Worker
}

func New(proxy gactors.Actor) *WorkPool {
	return &WorkPool{
		inbox:   make(chan *gactors.Query),
		proxy:   proxy,
		healthz: make(chan struct{}, 1),
		workers: make([]Worker, 0),
	}
}

func (s *WorkPool) Start(ctx context.Context) {
	for i := 0; i < runtime.NumCPU(); i++ {
		worker := Worker{
			id:      fmt.Sprintf("worker::%d", i),
			proxy:   s.proxy,
			inbox:   s.inbox,
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
	case s.inbox <- query:
	}
}

func (s *WorkPool) WaitAndStop(ctx context.Context) {
	log.Printf("%T stopping...\n", s.proxy)
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
	close(s.inbox)
	log.Printf("%T stopped\n", s.proxy)
}
