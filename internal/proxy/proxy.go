package proxy

import (
	"context"

	gdb "github.com/blong14/gache/internal/db"
	glog "github.com/blong14/gache/internal/logging"
)

type QueryProxy struct {
	inbox chan *gdb.Query
	pool  *WorkPool
}

func NewQueryProxy() (*QueryProxy, error) {
	inbox := make(chan *gdb.Query)
	return &QueryProxy{
		inbox: inbox,
		pool:  NewWorkPool(inbox),
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
	qp.pool.WaitAndStop(ctx)
	close(qp.inbox)
}
