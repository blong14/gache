package db

import (
	"context"
	"fmt"
	"github.com/blong14/gache/internal/db/arena"
	gerrors "github.com/blong14/gache/internal/errors"
)

type TableOpts struct {
	TableName []byte
	DataDir   []byte
	InMemory  bool
	WalMode   bool
}

type TableExecutor struct {
	table XRepository
}

func NewTable(opts *TableOpts) *TableExecutor {
	table := XNew(opts)
	return &TableExecutor{
		table: table,
	}
}

func (t *TableExecutor) Execute(_ context.Context, malloc arena.ByteArena, query *Query) {
	switch query.Header.Inst {
	case GetValue:
		var resp QueryResponse
		if value, ok := t.table.Get(query.Key); ok {
			resp = QueryResponse{
				Key:         query.Key,
				Value:       value,
				RangeValues: [][][]byte{{query.Key, value}},
				Stats: QueryStats{
					Count: 1,
				},
				Success: true,
			}
		}
		query.Done(resp)
	case Count:
		count := t.table.Count()
		query.Done(
			QueryResponse{
				RangeValues: [][][]byte{
					{[]byte("count"), []byte(fmt.Sprintf("%d", count))},
				},
				Stats: QueryStats{
					Count: uint(count),
				},
				Success: true,
			},
		)
	case GetRange:
		var resp QueryResponse
		values, ok := t.table.ScanWithLimit(
			query.KeyRange.Start, query.KeyRange.End, query.KeyRange.Limit)
		if ok {
			resp = QueryResponse{
				RangeValues: values,
				Stats: QueryStats{
					Count: uint(len(values)),
				},
				Success: true,
			}
		}
		query.Done(resp)
	case SetValue:
		var resp QueryResponse
		if err := t.table.XSet(malloc, query.Key, query.Value); err == nil {
			resp = QueryResponse{
				Key:   query.Key,
				Value: query.Value,
				Stats: QueryStats{
					Count: 1,
				},
				Success: true,
			}
		}
		query.Done(resp)
	case BatchSetValue:
		var errs *gerrors.Error
		for _, kv := range query.Values {
			if kv.Valid() {
				errs = gerrors.Append(errs, t.table.XSet(malloc, kv.Key, kv.Value))
			}
		}
		query.Done(
			QueryResponse{
				Stats: QueryStats{
					Count: uint(len(query.Values)),
				},
				Success: true,
			},
		)
	default:
	}
}
