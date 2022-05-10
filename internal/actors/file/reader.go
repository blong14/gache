package file

import (
	"context"
	"fmt"
	gactors "github.com/blong14/gache/internal/actors"
	gjson "github.com/blong14/gache/internal/io/file"
	glog "github.com/blong14/gache/logging"
	"log"
	"sync"
	"time"
)

func drain(ctx context.Context, wg *sync.WaitGroup, result <-chan *gactors.QueryResponse) {
	defer wg.Done()
	select {
	case <-ctx.Done():
	case <-result:
	}
}

// implements Actor interface
type loader struct {
	done   chan struct{}
	inbox  chan *gactors.Query
	outbox chan []*gactors.Query
}

func New() gactors.Streamer {
	return &loader{
		inbox:  make(chan *gactors.Query),
		outbox: make(chan []*gactors.Query),
		done:   make(chan struct{}),
	}
}

func (f *loader) Init(ctx context.Context) {
	glog.Track("%T waiting for work", f)
	defer glog.Track("%T stopped", f)
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.done:
			return
		case query, ok := <-f.inbox:
			if !ok || query.Header.Inst != gactors.Load {
				if query != nil {
					query.Finish(ctx)
				}
				return
			}
			start := time.Now()
			data, err := gjson.ReadCSV(string(query.Header.FileName))
			if err != nil {
				log.Fatal(err)
			}
			var wg sync.WaitGroup
			buffer := make([]*gactors.Query, 0, len(data))
			for _, kv := range data {
				wg.Add(1)
				setValue, result := gactors.NewSetValueQuery(query.Header.TableName, kv.Key, kv.Value)
				go drain(ctx, &wg, result)
				buffer = append(buffer, setValue)
			}
			go func(s time.Time) {
				defer query.Finish(ctx)
				wg.Wait()
				query.OnResult(ctx, gactors.QueryResponse{Success: true})
				fmt.Printf("load finished %s", time.Since(s))
			}(start)
			select {
			case <-ctx.Done():
			case <-f.done:
			case f.outbox <- buffer:
			}
		}
	}
}

func (f *loader) Close(_ context.Context) {
	close(f.done)
	close(f.inbox)
	close(f.outbox)
}

func (f *loader) OnResult() <-chan []*gactors.Query {
	out := make(chan []*gactors.Query)
	go func() {
		defer close(out)
		select {
		case <-f.done:
			return
		case out <- <-f.outbox:
		}
	}()
	return out
}

func (f *loader) Execute(ctx context.Context, row *gactors.Query) {
	select {
	case <-ctx.Done():
	case <-f.done:
	case f.inbox <- row:
	}
}
