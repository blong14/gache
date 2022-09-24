package view

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	glog "github.com/blong14/gache/internal/logging"

	gactors "github.com/blong14/gache/internal/actors"
	gwal "github.com/blong14/gache/internal/actors/wal"
	gcache "github.com/blong14/gache/internal/cache"
)

// Table implements Actor
type Table struct {
	log  *gwal.Log
	impl gcache.Table[[]byte, []byte]
	name []byte
}

func New(wal *gwal.Log, opts *gcache.TableOpts) gactors.Actor {
	return &Table{
		name: opts.TableName,
		log:  wal,
		impl: gcache.New[[]byte, []byte](bytes.Compare, bytes.Equal),
	}
}

func (va *Table) Send(ctx context.Context, query *gactors.Query) {
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
			fmt.Printf("%s\n", k)
			return true
		})
		query.Done(gactors.QueryResponse{Success: true})
	case gactors.SetValue:
		go va.log.Send(ctx, query)
		va.impl.Set(query.Key, query.Value)
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
				va.impl.Set(kv.Key, kv.Value)
			}
		}
		query.Done(gactors.QueryResponse{Success: true})
	case gactors.SumValues:
		var count uint64
		va.impl.Range(func(k uint64, v []byte) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			buf := bytes.NewReader(v)
			var value uint64
			err := binary.Read(buf, binary.BigEndian, &value)
			if err != nil {
				glog.Track("%s", err)
				return false
			}
			count += value
			return true
		})
		glog.Track("%d", count)
		var out []byte
		buf := bytes.NewBuffer(out)
		if err := binary.Write(buf, binary.BigEndian, count); err != nil {
			query.Done(gactors.QueryResponse{Success: false})
			return
		}
		query.Done(
			gactors.QueryResponse{
				Success: true,
				Value:   buf.Bytes(),
			},
		)
	default:
	}
}
