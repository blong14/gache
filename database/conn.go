package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/blong14/gache/internal/actors/proxy"
	gwal "github.com/blong14/gache/internal/actors/wal"
)

type QueryResponse struct {
	Key     []byte
	Value   []byte
	Success bool
}

type rows struct {
	next *QueryResponse
	done bool
}

func (r *rows) Columns() []string {
	return []string{"value"}
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	dest[0] = r.next
	r.done = true
	return nil
}

func (r *rows) HasNextResultSet() bool {
	hasNext := r.next != nil && !r.done
	return hasNext
}

func (r *rows) NextResultSet() error {
	if r.next == nil {
		return io.EOF
	}
	r.next = nil
	return nil
}

type conn struct {
	proxy *proxy.QueryProxy
}

func (c *conn) Commit() error {
	return errors.New("not implemented")
}

func (c *conn) Rollback() error {
	return errors.New("not implemented")
}

func (c *conn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *conn) Query(query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, err := parse(strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	for _, arg := range args {
		valueOrKey := arg.Value.([]byte)
		switch arg.Name {
		case "table":
			q.Header.TableName = valueOrKey
		case "key":
			q.Key = valueOrKey
		case "value":
			q.Value = valueOrKey
		default:
			return nil, errors.New("invalid args")
		}
	}
	c.proxy.Enqueue(ctx, q)
	resp := q.GetResponse()
	return &rows{
		next: &QueryResponse{
			Key:     resp.Key,
			Value:   resp.Value,
			Success: resp.Success,
		},
	}, nil
}

func (c *conn) Ping() error {
	result, err := c.Query(
		"select value from tables where key = %s;",
		[]driver.NamedValue{
			{Name: "key", Ordinal: 1, Value: "default"},
		},
	)
	if err != nil {
		return err
	}
	if err = result.Close(); err != nil {
		return err
	}
	return nil
}

var queryProxy *proxy.QueryProxy

const MEMORY = ":memory:"

type Driver struct {
	once sync.Once
}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	if dsn != MEMORY {
		return nil, errors.New("invalid dsn")
	}
	var err error
	d.once.Do(func() {
		var qp *proxy.QueryProxy
		qp, err = proxy.NewQueryProxy(gwal.New())
		queryProxy = qp
		proxy.StartProxy(context.Background(), queryProxy)
	})
	return &conn{proxy: queryProxy}, err
}

func init() {
	sql.Register("gache", &Driver{})
}

func GetProxy(db *sql.DB) (*proxy.QueryProxy, error) {
	return queryProxy, nil
}
