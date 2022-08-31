package client

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/rpc"
	"strings"

	gactors "github.com/blong14/gache/internal/actors"
	"github.com/blong14/gache/internal/actors/proxy"
	grepl "github.com/blong14/gache/internal/actors/replication"
	gwal "github.com/blong14/gache/internal/actors/wal"
)

type QueryResponse = gactors.QueryResponse

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
	conn  *rpc.Client
	proxy *proxy.QueryProxy
}

func (c conn) Commit() error {
	return errors.New("not implemented")
}

func (c conn) Rollback() error {
	return errors.New("not implemented")
}

func (c conn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c conn) Close() error {
	return c.conn.Close()
}

func (c conn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c conn) Query(query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, args)
}

func (c conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
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
	return &rows{next: resp}, nil
}

func (c conn) Ping() error {
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

type Driver struct {
	ctx   context.Context
	proxy *proxy.QueryProxy
}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	var wal *gwal.Log
	if dsn == "::memory::" {
		wal = gwal.New()
	} else {
		cn, err := rpc.DialHTTP("tcp", dsn)
		if err != nil {
			return nil, err
		}
		wal = gwal.New(grepl.New(cn))
	}
	qp, err := proxy.NewQueryProxy(wal)
	if err != nil {
		return nil, err
	}
	d.proxy = qp
	proxy.StartProxy(d.ctx, qp)
	return &conn{proxy: qp}, nil
}

func init() {
	sql.Register("gache", &Driver{ctx: context.Background()})
}

func GetProxy(db *sql.DB) *proxy.QueryProxy {
	return db.Driver().(*Driver).proxy
}
