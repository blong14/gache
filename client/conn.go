package client

import (
	"context"
	"errors"

	gdb "github.com/blong14/gache/internal/db"
	gproxy "github.com/blong14/gache/internal/proxy"
)

type Client interface {
	Close(ctx context.Context) error
	Get(ctx context.Context, t, k []byte) ([]byte, error)
	Set(ctx context.Context, t, k, v []byte) error
}

// proxyClient implements Client
type proxyClient struct {
	proxy *gproxy.QueryProxy
}

func NewProxyClient() Client {
	proxy, err := gproxy.NewQueryProxy()
	if err != nil {
		panic(err)
	}
	gproxy.StartProxy(context.Background(), proxy)
	return &proxyClient{proxy: proxy}
}

func (c *proxyClient) Get(ctx context.Context, table, key []byte) ([]byte, error) {
	query, _ := gdb.NewGetValueQuery(ctx, table, key)
	c.proxy.Send(ctx, query)
	resp := query.GetResponse()
	if !resp.Success {
		return nil, errors.New("missing value")
	}
	return resp.Value, nil
}

func (c *proxyClient) Set(ctx context.Context, table, key, value []byte) error {
	query, _ := gdb.NewSetValueQuery(ctx, table, key, value)
	c.proxy.Send(ctx, query)
	resp := query.GetResponse()
	if !resp.Success {
		return errors.New("key not set")
	}
	return nil
}

func (c *proxyClient) Close(ctx context.Context) error {
	gproxy.StopProxy(ctx, c.proxy)
	return nil
}

