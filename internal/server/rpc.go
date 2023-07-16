package server

import (
	"context"
	"errors"
	"net/rpc"
	"time"

	gdb "github.com/blong14/gache/internal/db"
	gerrors "github.com/blong14/gache/internal/errors"
	grpc "github.com/blong14/gache/internal/io/rpc"
	glog "github.com/blong14/gache/internal/logging"
	gproxy "github.com/blong14/gache/internal/proxy"
	gache "github.com/blong14/gache/sql"
)

var ErrNilClient = gerrors.NewGError(errors.New("nil client"))

type QueryService struct {
	Proxy *gproxy.QueryProxy
}

type QueryRequest struct {
	Queries []*gdb.Query
	Query   *gdb.Query
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
	qry := gdb.NewQuery(ctx, nil)
	qry.Header = query.Header
	qry.Key = query.Key
	qry.Value = query.Value
	qry.Values = query.Values

	qs.Proxy.Send(ctx, qry)
	r := qry.GetResponse()
	resp.Success = r.Success
	resp.Key = r.Key
	resp.Value = r.Value
	glog.Track("%T %v in %s", req, resp.Success, time.Since(start))
	return nil
}

func PublishQuery(client *rpc.Client, queries ...*gdb.Query) (*QueryResponse, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	req := new(QueryRequest)
	req.Query = queries[0]
	resp := new(QueryResponse)
	err := client.Call("QueryService.OnQuery", req, resp)
	return resp, err
}

func RPCHandlers() []grpc.Handler {
	proxy, err := gache.GetProxy()
	if err != nil {
		panic(err)
	}
	return []grpc.Handler{
		&QueryService{
			Proxy: proxy,
		},
	}
}
