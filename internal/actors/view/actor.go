package view

import (
	"bytes"
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	gactors "github.com/blong14/gache/internal/actors"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
)

// Table implements Actor
type Table struct {
	log    *gwal.Log
	tracer trace.Tracer
	impl   gcache.Table[[]byte, []byte]
	name   []byte
}

func New(wal *gwal.Log, opts *gcache.TableOpts) gactors.Actor {
	return &Table{
		name:   opts.TableName,
		tracer: otel.Tracer("table-proxy"),
		log:    wal,
		impl:   gcache.XNew[[]byte, []byte](bytes.Compare, bytes.Equal),
	}
}

func (va *Table) Execute(ctx context.Context, query *gactors.Query) {
	switch query.Header.Inst {
	case gactors.GetValue:
		var resp gactors.QueryResponse
		if value, ok := va.impl.Get(query.Key); ok {
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
		va.impl.Range(func(k, v []byte) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			fmt.Printf("%s ", k)
			return true
		})
		query.Done(gactors.QueryResponse{Success: true})
	case gactors.SetValue:
		// va.log.Execute(ctx, query)
		va.impl.Set(query.Key, query.Value)
		query.Done(gactors.QueryResponse{Success: true, Key: query.Key, Value: query.Value})
	case gactors.BatchSetValue:
		// va.log.Execute(ctx, query)
		for _, kv := range query.Values {
			if kv.Valid() {
				va.impl.Set(kv.Key, kv.Value)
			}
		}
		query.Done(gactors.QueryResponse{Success: true})
	default:
	}
}
