package sql

import (
	"context"
	"database/sql"
	"errors"
	"net/rpc"

	gdb "github.com/blong14/gache/internal/db"
)

type GacheClient interface {
	Get(ctx context.Context, t, k []byte) ([]byte, error)
	Scan(ctx context.Context, t, s, e []byte) ([][][]byte, error)
	ScanWithLimit(ctx context.Context, t, l []byte) ([][][]byte, error)
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
	var result *gdb.QueryResponse
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
	var result *gdb.QueryResponse
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

func (c *client) Scan(ctx context.Context, table, start, end []byte) ([][][]byte, error) {
	var result *gdb.QueryResponse
	err := c.database.QueryRowContext(
		ctx,
		"select * from :table where key between :start and :end;",
		sql.Named("table", table),
		sql.Named("start", start),
		sql.Named("end", end),
	).Scan(&result)
	if err != nil {
		return nil, err
	}
	if result.Success {
		return result.RangeValues, nil
	}
	return nil, errors.New("missing value")
}

func (c *client) ScanWithLimit(ctx context.Context, table, limit []byte) ([][][]byte, error) {
	var result *gdb.QueryResponse
	err := c.database.QueryRowContext(
		ctx,
		"select * from :table limit :limit;",
		sql.Named("table", table),
		sql.Named("limit", limit),
	).Scan(&result)
	if err != nil {
		return nil, err
	}
	if result.Success {
		return result.RangeValues, nil
	}
	return nil, errors.New("missing value")
}
