package view

import (
	"context"
	"fmt"

	gdb "github.com/blong14/gache/internal/db"
	gerrors "github.com/blong14/gache/internal/platform/errors"
)

type Table struct {
	impl gdb.Table
	name []byte
}

func New(opts *gdb.TableOpts) *Table {
	return &Table{
		name: opts.TableName,
		impl: gdb.New(opts),
	}
}

func (va *Table) Execute(ctx context.Context, query *gdb.Query) {
	switch query.Header.Inst {
	case gdb.GetValue:
		var resp gdb.QueryResponse
		if value, ok := va.impl.Get(query.Key); ok {
			resp = gdb.QueryResponse{
				Key:         query.Key,
				Value:       value,
				RangeValues: [][][]byte{{query.Key, value}},
				Stats: gdb.QueryStats{
					Count: 1,
				},
				Success: true,
			}
		}
		query.Done(resp)
	case gdb.Count:
		count := va.impl.Count()
		query.Done(
			gdb.QueryResponse{
				RangeValues: [][][]byte{
					{[]byte("count"), []byte(fmt.Sprintf("%d", count))},
				},
				Stats: gdb.QueryStats{
					Count: uint(count),
				},
				Success: true,
			},
		)
	case gdb.GetRange:
		var resp gdb.QueryResponse
		values, ok := va.impl.ScanWithLimit(
			query.KeyRange.Start, query.KeyRange.End, query.KeyRange.Limit)
		if ok {
			resp = gdb.QueryResponse{
				RangeValues: values,
				Stats: gdb.QueryStats{
					Count: uint(len(values)),
				},
				Success: true,
			}
		}
		query.Done(resp)
	case gdb.SetValue:
		var resp gdb.QueryResponse
		if err := va.impl.Set(query.Key, query.Value); err == nil {
			resp = gdb.QueryResponse{
				Key:   query.Key,
				Value: query.Value,
				Stats: gdb.QueryStats{
					Count: 1,
				},
				Success: true,
			}
		}
		query.Done(resp)
	case gdb.BatchSetValue:
		var errs *gerrors.Error
		for _, kv := range query.Values {
			if kv.Valid() {
				errs = gerrors.Append(errs, va.impl.Set(kv.Key, kv.Value))
			}
		}
		query.Done(
			gdb.QueryResponse{
				Stats: gdb.QueryStats{
					Count: uint(len(query.Values)),
				},
				Success: true,
			},
		)
	default:
	}
}

func (va *Table) Stop() {
	va.impl.Close()
}
