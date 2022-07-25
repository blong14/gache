package pool

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	gactors "github.com/blong14/gache/internal/actors"
	genv "github.com/blong14/gache/internal/environment"
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
	tracer  trace.Tracer
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
			var span trace.Span
			if genv.TraceEnabled() {
				ctx, span = s.tracer.Start(
					query.Context(), fmt.Sprintf("%T::%s Execute", s.proxy, s.id))
				span.SetAttributes(
					attribute.String(
						"query-instruction",
						query.Header.Inst.String(),
					),
				)
			}
			start := time.Now()
			s.proxy.Execute(ctx, query)
			glog.Track(
				"%T::%s executed %s values=%d in %s",
				s.proxy, s.id, query.Header.Inst, len(query.Values), time.Since(start),
			)
			if span != nil {
				span.End()
			}
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
	tracer := otel.Tracer("worker-pool")
	for i := 0; i < runtime.NumCPU(); i++ {
		worker := Worker{
			id:      fmt.Sprintf("worker::%d", i),
			proxy:   s.proxy,
			inbox:   s.inbox,
			stop:    make(chan interface{}, 0),
			healthz: s.healthz,
			tracer:  tracer,
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
