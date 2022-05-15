package proxy

import (
	"context"
	"errors"
	gerrors "github.com/blong14/gache/errors"
	gactor "github.com/blong14/gache/internal/actors"
	grpc "github.com/blong14/gache/internal/io/rpc"
	glog "github.com/blong14/gache/logging"
	"net/rpc"
)

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
	glog.Track("%T %d", req, len(req.Queries))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, query := range req.Queries {
		q.Proxy.Execute(ctx, query)
		go func(qry *gactor.Query) {
			_ = gactor.GetQueryResult(ctx, qry)
		}(query)
	}
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
