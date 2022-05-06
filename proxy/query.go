package proxy

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	gerrors "github.com/blong14/gache/errors"
	gactor "github.com/blong14/gache/internal/actors"
	gcache "github.com/blong14/gache/internal/cache"
	gtree "github.com/blong14/gache/internal/cache/sorted/treemap"
	grpc "github.com/blong14/gache/internal/io/rpc"
	glog "github.com/blong14/gache/logging"
	gwal "github.com/blong14/gache/proxy/wal"
)

// QueryProxy implements gactors.Actor interface
type QueryProxy struct {
	inbox chan *gactor.Query
	log   *gwal.WAL
	// table name to table actor
	tables *gtree.TreeMap[[]byte, gactor.Actor]
}

// NewQueryProxy returns a fully ready to use *QueryProxy
func NewQueryProxy(wal *gwal.WAL) (*QueryProxy, error) {
	return &QueryProxy{
		log:    wal,
		inbox:  make(chan *gactor.Query),
		tables: gtree.New[[]byte, gactor.Actor](bytes.Compare),
	}, nil
}

func (qp *QueryProxy) Start(parentCtx context.Context) {
	glog.Track("%T waiting for work", qp)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case query, ok := <-qp.inbox:
			if !ok {
				return
			}
			switch query.Header.Inst {
			case gactor.AddTable:
				table := gactor.NewTableActor(&gcache.TableOpts{
					TableName: query.Header.TableName,
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
				go table.Start(ctx)
				qp.tables.Set(query.Header.TableName, table)
				go func() {
					defer query.Finish()
					query.OnResult(ctx, gactor.QueryResponse{
						Key:     nil,
						Value:   nil,
						Success: true,
					})
				}()
			case gactor.GetValue, gactor.SetValue:
				glog.Track("%v", query)
				table, ok := qp.tables.Get(query.Header.TableName)
				if !ok {
					continue
				}
				go table.Execute(ctx, query)
			case gactor.Load:
				loader, ok := qp.tables.Get([]byte("file-loader"))
				if !ok {
					loader = gactor.NewFileActor()
					go loader.Start(ctx)
					qp.tables.Set([]byte("file-loader"), loader)
				}
				go loader.Execute(ctx, query)
				go func() {
					for query := range loader.(gactor.Streamer).OnResult() {
						qp.Execute(ctx, query)
					}
				}()
			default:
				panic("should not happen")
			}
		}
	}
}

func (qp *QueryProxy) Stop(ctx context.Context) {
	glog.Track("%T stopping...", qp)
	close(qp.inbox)
	qp.log.Stop(ctx)
	qp.tables.Range(func(_, v any) bool {
		sub, ok := v.(gactor.Actor)
		if !ok {
			return true
		}
		sub.Stop(ctx)
		return true
	})
	glog.Track("%T stopped", qp)
}

func (qp *QueryProxy) Execute(ctx context.Context, query *gactor.Query) {
	select {
	case <-ctx.Done():
	case qp.inbox <- query:
	}
}

// implements Actor interface
type queryReplicator struct {
	client *rpc.Client
	errs   *gerrors.Error
	inbox  chan *gactor.Query
}

func NewQuerySubscriber(client *rpc.Client) gactor.Actor {
	return &queryReplicator{
		client: client,
		inbox:  make(chan *gactor.Query),
	}
}

func (r *queryReplicator) Start(ctx context.Context) {
	glog.Track("%T waiting for work", r)
	defer glog.Track("%T stopped", r)
	if r.client == nil {
		var err error
		r.client, err = grpc.Client("localhost:8080")
		if err != nil {
			log.Println(err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case query, ok := <-r.inbox:
			if !ok {
				return
			}
			r.errs = gerrors.Append(
				r.errs,
				gerrors.OnlyError(PublishQuery(r.client, query)),
			)
			glog.Track("%#v", r.errs)
		}
	}
}

func (r *queryReplicator) Stop(_ context.Context) {
	glog.Track("%T stopping...", r)
	close(r.inbox)
}

func (r *queryReplicator) Execute(ctx context.Context, query *gactor.Query) {
	select {
	case <-ctx.Done():
	case r.inbox <- query:
	}
}

var onc sync.Once

func StartProxy(ctx context.Context, qp *QueryProxy) {
	onc.Do(func() {
		log.Println("starting query proxy")
		go qp.log.Start(ctx)
		go qp.Start(ctx)
		query, done := gactor.NewAddTableQuery([]byte("default"))
		qp.Execute(ctx, query)
		<-done
	})
}

func StopProxy(ctx context.Context, qp *QueryProxy) {
	log.Println("stopping query proxy")
	qp.Stop(ctx)
}

var ErrNilClient = gerrors.NewGError(errors.New("nil client"))

type QueryService struct {
	Proxy *QueryProxy
}

type QueryRequest struct {
	Queries []*gactor.Query
}

type QueryResponse struct {
	Success bool
}

func (q *QueryService) OnQuery(req *QueryRequest, resp *QueryResponse) error {
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for _, query := range req.Queries {
			q.Proxy.Execute(ctx, query)
		}
	}()
	resp.Success = true
	return nil
}

func PublishQuery(client *rpc.Client, queries ...*gactor.Query) (*QueryResponse, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	req := new(QueryRequest)
	req.Queries = queries
	resp := new(QueryResponse)
	err := gerrors.Append(client.Call("QueryService.OnQuery", req, resp))
	return resp, err
}

func RpcHandlers(proxy *QueryProxy) []grpc.Handler {
	return []grpc.Handler{
		&QueryService{
			Proxy: proxy,
		},
	}
}
