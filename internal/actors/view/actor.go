package view

import (
	"context"
	"fmt"

	gactors "github.com/blong14/gache/internal/actors"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
)

// Table implements Actor
type Table struct {
	log  *gwal.Log
	impl gcache.Table[uint64, []byte]
	name []byte
}

func New(wal *gwal.Log, opts *gcache.TableOpts) gactors.Actor {
	return &Table{
		name: opts.TableName,
		log:  wal,
		impl: gcache.New[uint64, []byte](gcache.Uint64Compare, gcache.Uint64Equals),
	}
}

func (va *Table) Send(ctx context.Context, query *gactors.Query) {
	switch query.Header.Inst {
	case gactors.GetValue:
		var resp gactors.QueryResponse
		if value, ok := va.impl.Get(gcache.Hash(query.Key)); ok {
			resp = gactors.QueryResponse{
				Key:     query.Key,
				Value:   value,
				Success: true,
			}
		}
		query.Done(resp)
	case gactors.Print:
		va.impl.Print()
		query.Done(gactors.QueryResponse{Success: true})
	case gactors.Range:
		va.impl.Range(func(k uint64, v []byte) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			fmt.Printf("%v\n", k)
			return true
		})
		query.Done(gactors.QueryResponse{Success: true})
	case gactors.SetValue:
		go va.log.Send(ctx, query)
		va.impl.Set(gcache.Hash(query.Key), query.Value)
		query.Done(
			gactors.QueryResponse{
				Key:     query.Key,
				Value:   query.Value,
				Success: true,
			},
		)
	case gactors.BatchSetValue:
		go va.log.Send(ctx, query)
		for _, kv := range query.Values {
			if kv.Valid() {
				va.impl.Set(gcache.Hash(query.Key), kv.Value)
			}
		}
		query.Done(gactors.QueryResponse{Success: true})
	default:
	}
}
