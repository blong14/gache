package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	gdb "github.com/blong14/gache/internal/db"
	glog "github.com/blong14/gache/internal/platform/logging"
	gproxy "github.com/blong14/gache/internal/proxy"
)

type QueryResponse struct {
	Key         []byte
	Value       []byte
	RangeValues [][][]byte
	Stats       gdb.QueryStats
	Success     bool
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
	proxy *gproxy.QueryProxy
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
	glog.Track("closing db connection...")
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
		case "start":
			q.KeyRange.Start = valueOrKey
		case "end":
			q.KeyRange.End = valueOrKey
		case "key":
			q.Key = valueOrKey
		case "value":
			q.Value = valueOrKey
		case "limit":
			q.KeyRange.Limit, err = strconv.Atoi(string(valueOrKey))
			if err != nil {
				return nil, fmt.Errorf("invalid limit arg: %w", err)
			}
		default:
			return nil, errors.New("invalid args")
		}
	}
	c.proxy.Send(ctx, q)
	resp := q.GetResponse()
	return &rows{
		next: &QueryResponse{
			Key:         resp.Key,
			Value:       resp.Value,
			RangeValues: resp.RangeValues,
			Stats:       resp.Stats,
			Success:     resp.Success,
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

var queryProxy *gproxy.QueryProxy

const MEMORY = ":memory:"

type Driver struct {
	once sync.Once
}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	var err error
	d.once.Do(func() {
		var qp *gproxy.QueryProxy
		qp, err = gproxy.NewQueryProxy()
		queryProxy = qp
		gproxy.StartProxy(context.Background(), queryProxy)
	})
	return &conn{proxy: queryProxy}, err
}

func init() {
	sql.Register("gache", &Driver{})
}

func GetProxy() (*gproxy.QueryProxy, error) {
	return queryProxy, nil
}
