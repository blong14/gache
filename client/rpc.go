package client

import (
	"context"
	"database/sql"
	"errors"
	"net/rpc"

	"github.com/blong14/gache/internal/actors"
)

type GacheClient interface {
	Get(ctx context.Context, t, k []byte) ([]byte, error)
	Set(ctx context.Context, t, k, v []byte) error
}

// client implements GacheClient
type client struct {
	conn     *rpc.Client
	database *sql.DB
}

func New(c *rpc.Client, db *sql.DB) GacheClient {
	return &client{
		conn:     c,
		database: db,
	}
}

func (c *client) Get(ctx context.Context, table, key []byte) ([]byte, error) {
	var result *actors.QueryResponse
	err := c.database.QueryRowContext(
		ctx,
		"select value from :table where key = :key",
		sql.Named("table", table),
		sql.Named("key", key),
	).Scan(&result)
	if err != nil {
		return nil, err
	}
	if result.Success {
		return result.Value, nil
	}
	return nil, errors.New("missing value")
}

func (c *client) Set(ctx context.Context, table, key, value []byte) error {
	var result *actors.QueryResponse
	err := c.database.QueryRowContext(
		ctx,
		"insert into :table set key = :key, value = :value",
		sql.Named("table", table),
		sql.Named("key", key),
		sql.Named("value", value),
	).Scan(&result)
	if err != nil {
		return err
	}
	return nil
}
