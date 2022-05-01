package proxy

import (
	"bytes"
	"context"
	"log"
	"sync"

	gactor "github.com/blong14/gache/internal/actors"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	glog "github.com/blong14/gache/logging"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	// table name to table actor
	tables *gtree.TreeMap[[]byte, gactor.TableActor]
	inbox  chan *gactor.Query
}

// NewQueryProxy returns a fully ready to use *QueryProxy
// TODO(ben): update signature; remove error
func NewQueryProxy() (*QueryProxy, error) {
	return &QueryProxy{
		inbox:  make(chan *gactor.Query),
		tables: gtree.New[[]byte, gactor.TableActor](bytes.Compare),
	}, nil
}

func (qp *QueryProxy) Start(ctx context.Context) {
	glog.Track("%T Waiting for work", qp)
	for {
		select {
		case <-ctx.Done():
			return
		case query, ok := <-qp.inbox:
			if !ok {
				continue
			}
			switch query.Header.Inst {
			case gactor.AddTable:
				table := gactor.NewTableActor()
				// TOD(Ben): Fix goroutine leak; should make a call to table.Stop(ctx) when shutting
				// down the query proxy
				go table.Start(ctx)
				qp.tables.Set(query.Header.TableName, table)
				continue
			case gactor.GetValue:
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					continue
				}
				glog.Track("%T Get key=%s", table, string(query.Key))
				go table.Get(context.TODO(), query)
				continue
			case gactor.SetValue:
				view, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					continue
				}
				go view.Set(context.TODO(), query)
				continue
			default:
				panic("should not happen")
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

var onc sync.Once

func StartProxy(ctx context.Context, qp *QueryProxy) {
	onc.Do(func() {
		log.Println("starting query proxy")
		go qp.Start(ctx)
		query := &gactor.Query{
			Header: gactor.QueryHeader{
				TableName: []byte("default"),
				Inst:      gactor.AddTable,
			},
		}
		qp.Set(ctx, query)
	})
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stoping query proxy")
	qp.Stop(ctx)
}
