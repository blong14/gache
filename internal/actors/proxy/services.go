package proxy

import (
	"context"
	"errors"
	"net/rpc"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	gactor "github.com/blong14/gache/internal/actors"
	genv "github.com/blong14/gache/internal/environment"
	gerrors "github.com/blong14/gache/internal/errors"
	grpc "github.com/blong14/gache/internal/io/rpc"
	glog "github.com/blong14/gache/internal/logging"
)

var ErrNilClient = gerrors.NewGError(errors.New("nil client"))

type QueryService struct {
	Proxy  *QueryProxy
	Tracer trace.Tracer
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
	ctx := context.Background()
	var span trace.Span
	if genv.TraceEnabled() {
		ctx, span = qs.Tracer.Start(ctx, "query-service:OnQuery")
		defer span.End()
		span.SetAttributes(
			attribute.Int("query-length", len(req.Queries)),
			attribute.String("query-instruction", req.Query.Header.Inst.String()),
		)
	}

	done := make(chan gactor.QueryResponse, 1)
	defer close(done)
	query := req.Query
	qry := gactor.NewQuery(ctx, done)
	qry.Header = query.Header
	qry.Key = query.Key
	qry.Value = query.Value
	qry.Values = query.Values

	qs.Proxy.Enqueue(ctx, qry)
	select {
	case <-ctx.Done():
	case result, ok := <-done:
		if !ok {
			break
		}
		resp.Success = result.Success
	}
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
	err := gerrors.Append(client.Call("QueryService.OnQuery", req, resp))
	return resp, err
}

func RpcHandlers(proxy *QueryProxy) []grpc.Handler {
	return []grpc.Handler{
		&QueryService{
			Tracer: otel.Tracer("query-service"),
			Proxy:  proxy,
		},
	}
}
