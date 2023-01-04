package view

import (
	"context"
	"fmt"
	"log"

	gdb "github.com/blong14/gache/internal/db"
	gerrors "github.com/blong14/gache/internal/errors"
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
		va.impl.Range(func(k, v []byte) bool {
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
		if err := va.impl.Set(query.Key, query.Value); err != nil {
			query.Done(gdb.QueryResponse{Success: false})
			return
		}
		query.Done(
			gdb.QueryResponse{
				Key:     query.Key,
				Value:   query.Value,
				Success: true,
			},
		)
	case gdb.BatchSetValue:
		var errs *gerrors.Error
		for _, kv := range query.Values {
			if kv.Valid() {
				errs = gerrors.Append(errs, va.impl.Set(kv.Key, kv.Value))
			}
		}
		if errs.ErrorOrNil() != nil {
			log.Println(errs)
		}
		query.Done(gdb.QueryResponse{Success: true})
	default:
	}
}

func (va *Table) Stop() {
	va.impl.Close()
}
