package proxy

import (
	"bytes"
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	gactor "github.com/blong14/gache/internal/actors"
	gcache "github.com/blong14/gache/internal/cache"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	gwal "github.com/blong14/gache/internal/cache/wal"
	glog "github.com/blong14/gache/logging"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	inbox chan *gactor.Query
	log   *gwal.WAL
	// table name to table actor
	tables *gtree.TreeMap[[]byte, gactor.Actor]
}

// NewQueryProxy returns a fully ready to use *QueryProxy
// TODO(ben): update signature; remove error
func NewQueryProxy() (*QueryProxy, error) {
	return &QueryProxy{
		log:    gwal.New(),
		inbox:  make(chan *gactor.Query),
		tables: gtree.New[[]byte, gactor.Actor](bytes.Compare),
	}, nil
}

func (qp *QueryProxy) Start(parentCtx context.Context) {
	glog.Track("%T Waiting for work", qp)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case query, ok := <-qp.inbox:
			if !ok {
				continue
			}
			if err := qp.log.Write(query); err != nil {
				log.Println(err)
				// TODO(ben): should think about how to handle this better
				continue
			}
			switch query.Header.Inst {
			case gactor.AddTable:
				table := gactor.NewTableActor(&gcache.TableOpts{
					WithCache: func() *gtree.TreeMap[[]byte, []byte] {
						start := time.Now()
						impl := gtree.New[[]byte, []byte](bytes.Compare)
						for i := 0; i < 1_000_000; i++ {
							impl.Set([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
						}
						glog.Track("startup=%s", time.Since(start))
						return impl
					},
				})
				// TOD(Ben): Fix goroutine leak; should make a call to table.Stop(ctx) when shutting
				// down the query proxy
				go table.Start(ctx)
				qp.tables.Set(query.Header.TableName, table)
				query.OnResult(ctx, gactor.QueryResponse{
					Key:     nil,
					Value:   nil,
					Success: true,
				})
			case gactor.GetValue, gactor.SetValue:
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					continue
				}
				go table.Execute(ctx, query)
			default:
				panic("should not happen")
			}
		}
	}
}

func (qp *QueryProxy) Stop(_ context.Context) {
	close(qp.inbox)
}

func (qp *QueryProxy) Execute(ctx context.Context, query *gactor.Query) {
	select {
	case <-ctx.Done():
	case qp.inbox <- query:
	}
}

var onc sync.Once

func StartProxy(ctx context.Context, qp *QueryProxy) {
	onc.Do(func() {
		log.Println("starting query proxy")
		go qp.Start(ctx)
	})
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stoping query proxy")
	qp.Stop(ctx)
}
