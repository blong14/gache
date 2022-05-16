package file

import (
	"context"
	gactors "github.com/blong14/gache/internal/actors"
	gjson "github.com/blong14/gache/internal/io/file"
	glog "github.com/blong14/gache/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"log"
	"sync"
)

func drain(ctx context.Context, wg *sync.WaitGroup, result <-chan *gactors.QueryResponse) {
	defer wg.Done()
	select {
	case <-ctx.Done():
	case <-result:
		trace.SpanFromContext(ctx).End()
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
			spanCtx, span := otel.Tracer("").Start(query.Context(), "query-reader:Read")
			data, err := gjson.TraceReadCSV(spanCtx, string(query.Header.FileName))
			if err != nil {
				span.End()
				log.Fatal(err)
			}
			var wg sync.WaitGroup
			buffer := make([]*gactors.Query, 0, len(data))
			tr := otel.Tracer("")
			for i, kv := range data {
				wg.Add(1)
				childCtx, _ := tr.Start(spanCtx, "query-reader:gactors.Load:SetValueQuery")
				var setValue *gactors.Query
				var result <-chan *gactors.QueryResponse
				if (i < 10) || (i > 18765) {
					setValue, result = gactors.TraceNewSetValueQuery(childCtx, query.Header.TableName, kv.Key, kv.Value)
				} else {
					setValue, result = gactors.NewSetValueQuery(query.Header.TableName, kv.Key, kv.Value)
				}
				go drain(childCtx, &wg, result)
				buffer = append(buffer, setValue)
			}
			go func(ctx context.Context) {
				spanCtx, span := tr.Start(ctx, "query-reader:gactors.Load:OnResult")
				defer span.End()
				defer query.Finish(spanCtx)
				wg.Wait()
				query.OnResult(spanCtx, gactors.QueryResponse{Success: true})
			}(spanCtx)
			select {
			case <-ctx.Done():
			case <-f.done:
			case f.outbox <- buffer:
			}
			span.End()
		}
	}
}

func (f *loader) Close(ctx context.Context) {
	_, span := otel.Tracer("").Start(ctx, "query-loader:Close")
	defer span.End()
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

func (f *loader) Execute(ctx context.Context, query *gactors.Query) {
	spanCtx, span := otel.Tracer("").Start(ctx, "query-loader:Execute")
	defer span.End()
	select {
	case <-spanCtx.Done():
	case <-f.done:
	case f.inbox <- query:
	}
}
