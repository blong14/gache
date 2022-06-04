package reader

import (
	"context"
	"log"

	gactors "github.com/blong14/gache/internal/actors"
	gfile "github.com/blong14/gache/internal/io/file"
)

// Reader implements Actor interface
type Reader struct {
	table gactors.Actor
	scnr  *gfile.Scanner
}

func New(table gactors.Actor) gactors.Actor {
	return &Reader{
		table: table,
		scnr:  gfile.NewScanner(),
	}
}

func (f *Reader) Execute(ctx context.Context, query *gactors.Query) {
	if f.table == nil || query.Header.Inst != gactors.Load {
		if query != nil {
			query.Done(gactors.QueryResponse{Success: false})
		}
		return
	}
	buffer, err := gfile.ReadCSV(string(query.Header.FileName))
	if err != nil {
		log.Fatal(err)
	}
	f.scnr.Init(buffer)
	for f.scnr.Scan() {
		go func(rows []gactors.KeyValue) {
			q, done := gactors.NewBatchSetValueQuery(ctx, query.Header.TableName, f.scnr.Rows())
			f.table.Execute(q.Context(), q)
			close(done)
		}(f.scnr.Rows())
	}
	query.Done(gactors.QueryResponse{Success: true})
	return
}
