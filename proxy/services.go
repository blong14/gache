package proxy

import (
	"context"
	"errors"
	"net/rpc"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	gerrors "github.com/blong14/gache/errors"
	gactor "github.com/blong14/gache/internal/actors"
	grpc "github.com/blong14/gache/internal/io/rpc"
	glog "github.com/blong14/gache/logging"
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
}

func (qs *QueryService) OnQuery(req *QueryRequest, resp *QueryResponse) error {
	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	tr := otel.Tracer("query-service")
	spanCtx, span := tr.Start(ctx, "query-service:OnQuery")
	span.SetAttributes(attribute.Int("query-length", len(req.Queries)))
	query := req.Query
	qry := gactor.TraceNewQuery(spanCtx)
	qry.Header = query.Header
	qry.Key = query.Key
	qry.Value = query.Value
	qs.Proxy.Execute(spanCtx, &qry)
	_ = gactor.GetQueryResult(spanCtx, &qry)
	resp.Success = true
	glog.Track("%T in %s", req, time.Since(start))
	span.End()
	cancel()
	return nil
}

func PublishQuery(client *rpc.Client, queries ...*gactor.Query) (*QueryResponse, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	req := new(QueryRequest)
	req.Query = queries[0]
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
