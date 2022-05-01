package proxy

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	gactor "github.com/blong14/gache/internal/actors"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	glog "github.com/blong14/gache/logging"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	views gactor.WriteActor

	databases gactor.ViewActor

	inbox chan *gactor.Query

	// TODO: remove these
	ccache *gtree.CTreeMap
	gcache *gtree.TreeMap[string, string]
	scache *sync.Map
}

func NewQueryProxy() (*QueryProxy, error) {
	ccache := gtree.NewCTreeMap()
	gcache := gtree.New[string, string](func(a, b string) int {
		if b < a {
			return -1
		} else if b == a {
			return 0
		} else {
			return 1
		}
	})
	scache := &sync.Map{}
	start := time.Now()
	for i := 0; i < 1_000_000; i++ {
		gcache.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	log.Printf("gcache count %d time %s\n", gcache.Size(), time.Since(start))
	start = time.Now()
	for i := 0; i < 1_000_000; i++ {
		gtree.Set(ccache, strconv.Itoa(i), strconv.Itoa(i))
	}
	log.Printf("ccache count %d time %s\n", gtree.Size(ccache), time.Since(start))
	start = time.Now()
	count := 0
	for i := 0; i < 1_000_000; i++ {
		if _, ok := scache.LoadOrStore(strconv.Itoa(i), strconv.Itoa(i)); !ok {
			count++
		}
	}
	log.Printf("scache count %d time %s\n", count, time.Since(start))

	return &QueryProxy{
		gcache: gcache,
		ccache: ccache,
		scache: scache,
		inbox:  make(chan *gactor.Query),
	}, nil
}

func (qp *QueryProxy) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println(ctx)
			return
		case query := <-qp.inbox:
			switch cmd := query.CMD; cmd {
			case gactor.AddIndex:
				if query.Index != nil {
					qp.views.Set(context.TODO(), query)
				}
				continue
			case gactor.GetValue:
				glog.Track("spawning view actor")

				start := time.Now()
				value, ok := qp.gcache.Get(string(query.Key))
				log.Printf("gcache.Get %s", time.Since(start))
				go query.OnResult(
					ctx,
					gactor.Response{
						Key:     query.Key,
						Value:   []byte(value),
						Success: ok,
					},
				)
				continue
			}
		}
	}
}

func (qp *QueryProxy) Stop(_ context.Context) {
	close(qp.inbox)
}

func (qp *QueryProxy) Get(ctx context.Context, query *gactor.Query) {
	select {
	case <-ctx.Done():
	case qp.inbox <- query:
	}
}

func (qp *QueryProxy) Set(ctx context.Context, query *gactor.Query) {
	select {
	case <-ctx.Done():
	case qp.inbox <- query:
	}
}

func (qp *QueryProxy) Range(ctx context.Context, fnc func(k, v any) bool) {}

func StartProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("starting query proxy")
	qp.Start(ctx)
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stoping query proxy")
	qp.Stop(ctx)
}
