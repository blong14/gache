package proxy

import (
	"context"

	gdb "github.com/blong14/gache/internal/db"
	glog "github.com/blong14/gache/internal/logging"
	gpool "github.com/blong14/gache/internal/proxy/pool"
)

type QueryProxy struct {
	pool *gpool.WorkPool
}

func NewQueryProxy() (*QueryProxy, error) {
	return &QueryProxy{
		pool: gpool.New(),
	}, nil
}

func (qp *QueryProxy) Send(ctx context.Context, query *gdb.Query) {
	qp.pool.Send(ctx, query)
}

func StartProxy(ctx context.Context, qp *QueryProxy) {
	glog.Track("starting query proxy")
	qp.pool.Start(ctx)
	for _, table := range []string{"default"} {
		query, done := gdb.NewAddTableQuery(ctx, []byte(table))
		qp.Send(ctx, query)
		<-done
	}
	glog.Track("default tables added")
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	glog.Track("stopping query proxy")
	gpool.WaitAndStop(ctx, qp.pool)
}
