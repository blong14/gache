package proxy

import (
	"context"
	"errors"
	"net/rpc"
	"time"

	gactor "github.com/blong14/gache/internal/actors"
	gerrors "github.com/blong14/gache/internal/errors"
	grpc "github.com/blong14/gache/internal/io/rpc"
	glog "github.com/blong14/gache/internal/logging"
)

var ErrNilClient = gerrors.NewGError(errors.New("nil client"))

type QueryService struct {
	Proxy *QueryProxy
}

type QueryRequest struct {
	Queries []*gactor.Query
	Query   *gactor.Query
}

type QueryResponse struct {
	Success bool
	Key     []byte
	Value   []byte
}

func (qs *QueryService) OnQuery(req *QueryRequest, resp *QueryResponse) error {
	start := time.Now()
	ctx := context.Background()
	query := req.Query
	qry := gactor.NewQuery(ctx, nil)
	qry.Header = query.Header
	qry.Key = query.Key
	qry.Value = query.Value
	qry.Values = query.Values

	qs.Proxy.Enqueue(ctx, qry)
	r := qry.GetResponse()
	resp.Success = r.Success
	resp.Key = r.Key
	resp.Value = r.Value
	glog.Track("%T %v in %s", req, resp.Success, time.Since(start))
	return nil
}

func PublishQuery(client *rpc.Client, queries ...*gactor.Query) (*QueryResponse, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	req := new(QueryRequest)
	req.Query = queries[0]
	resp := new(QueryResponse)
	err := client.Call("QueryService.OnQuery", req, resp)
	return resp, err
}

func RpcHandlers(proxy *QueryProxy) []grpc.Handler {
	return []grpc.Handler{
		&QueryService{
			Proxy: proxy,
		},
	}
}
