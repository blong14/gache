package proxy

import (
	"context"
	"errors"
	"net/rpc"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	gactor "github.com/blong14/gache/internal/actors"
	genv "github.com/blong14/gache/internal/environment"
	gerrors "github.com/blong14/gache/internal/errors"
	grpc "github.com/blong14/gache/internal/io/rpc"
	grate "github.com/blong14/gache/internal/limiter"
	glog "github.com/blong14/gache/internal/logging"
)

var ErrNilClient = gerrors.NewGError(errors.New("nil client"))

type QueryService struct {
	Proxy   *QueryProxy
	Limiter grate.RateLimiter
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
	defer cancel()

	query := req.Query
	var span trace.Span
	if genv.TraceEnabled() {
		tr := otel.Tracer("query-service")
		ctx, span = tr.Start(ctx, "query-service:OnQuery")
		span.SetAttributes(
			attribute.Int("query-length", len(req.Queries)),
			attribute.String("query-instruction", req.Query.Header.Inst.String()),
		)
	}

	done := make(chan *gactor.Query, 1)
	defer close(done)
	qry := gactor.NewQuery(ctx, done)
	qry.Header = query.Header
	qry.Key = query.Key
	qry.Value = query.Value

	err := qs.Limiter.Wait(ctx)
	if err != nil {
		return gerrors.NewGError(err)
	}

	go qs.Proxy.Execute(ctx, qry)
	select {
	case <-ctx.Done():
	case result, ok := <-done:
		if !ok {
			break
		}
		if result != nil {
			resp.Success = result.GetResponse().Success
		}
	}
	glog.Track("%T in %s", req, time.Since(start))
	span.End()
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
			Limiter: grate.MultiLimiter(
				rate.NewLimiter(
					grate.Per(100, time.Millisecond),
					grate.Burst(100),
				),
			),
			Proxy: proxy,
		},
	}
}
