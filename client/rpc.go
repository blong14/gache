package client

import (
	"context"
	"net/rpc"

	"github.com/blong14/gache/internal/actors"
	"github.com/blong14/gache/internal/actors/proxy"
)

type GacheClient interface {
	Get(ctx context.Context, k []byte) ([]byte, error)
	Set(ctx context.Context, k, v []byte) error
}

// client implements GacheClient
type client struct {
	conn *rpc.Client
	db   []byte
}

func New(c *rpc.Client, db []byte) GacheClient {
	return &client{
		conn: c,
		db:   db,
	}
}

func (c *client) Get(ctx context.Context, key []byte) ([]byte, error) {
	query, done := actors.NewGetValueQuery(ctx, c.db, key)
	defer close(done)
	resp, err := proxy.PublishQuery(c.conn, query)
	if err != nil {
		return []byte{}, err
	}
	return resp.Value, nil
}

func (c *client) Set(ctx context.Context, key, value []byte) error {
	query, done := actors.NewSetValueQuery(ctx, c.db, key, value)
	defer close(done)
	_, err := proxy.PublishQuery(c.conn, query)
	if err != nil {
		return err
	}
	return nil
}
