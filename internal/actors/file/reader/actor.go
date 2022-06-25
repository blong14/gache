package reader

import (
	"context"
	gactors "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/io/file"
	gpool "github.com/blong14/gache/internal/pool"
)

// Reader implements Actor interface
type Reader struct {
	table gactors.Actor
	pool  *gpool.WorkPool
	batch int
}

func New(table gactors.Actor) gactors.Actor {
	pool := gpool.New(table)
	pool.Start(context.Background())
	return &Reader{
		table: table,
		pool:  pool,
	}
}

func (f *Reader) Execute(ctx context.Context, query *gactors.Query) {
	if f.table == nil || query.Header.Inst != gactors.Load {
		if query != nil {
			query.Done(gactors.QueryResponse{Success: false})
		}
		return
	}
	reader := gfile.ScanCSV(string(query.Header.FileName))
	reader.Init()
	for reader.Scan() {
		q := gactors.NewQuery(ctx, nil)
		q.Header = gactors.QueryHeader{
			TableName: query.Header.TableName,
			Inst:      gactors.BatchSetValue,
		}
		q.Values = reader.Rows()
		f.pool.Send(ctx, q)
	}
	f.pool.Wait(ctx)
	reader.Close()
	success := false
	if err := reader.Err(); err == nil {
		success = true
	}
	query.Done(
		gactors.QueryResponse{
			Success: success,
			Value:   []byte("done"),
		},
	)
	return
}
