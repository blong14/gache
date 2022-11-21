package view

import (
	"context"
	"fmt"

	gdb "github.com/blong14/gache/internal/db"
)

type Table struct {
	impl *gdb.TableCache
	name []byte
}

func New(opts *gdb.TableOpts) *Table {
	impl := gdb.New()
	return &Table{
		name: opts.TableName,
		impl: impl,
	}
}

func (va *Table) Execute(ctx context.Context, query *gdb.Query) {
	switch query.Header.Inst {
	case gdb.GetValue:
		var resp gdb.QueryResponse
		if value, ok := va.impl.Get(query.Key); ok {
			resp = gdb.QueryResponse{
				Key:     query.Key,
				Value:   value,
				Success: true,
			}
		}
		query.Done(resp)
	case gdb.Print:
		va.impl.Print()
		query.Done(gdb.QueryResponse{Success: true})
	case gdb.Range:
		va.impl.Range(func(k uint64, v []byte) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			fmt.Printf("%v\n", k)
			return true
		})
		query.Done(gdb.QueryResponse{Success: true})
	case gdb.SetValue:
		va.impl.Set(query.Key, query.Value)
		query.Done(
			gdb.QueryResponse{
				Key:     query.Key,
				Value:   query.Value,
				Success: true,
			},
		)
	case gdb.BatchSetValue:
		for _, kv := range query.Values {
			if kv.Valid() {
				va.impl.Set(query.Key, kv.Value)
			}
		}
		query.Done(gdb.QueryResponse{Success: true})
	default:
	}
}
