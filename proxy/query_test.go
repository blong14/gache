package proxy_test

import (
	"context"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gproxy "github.com/blong14/gache/proxy"
	gwal "github.com/blong14/gache/proxy/wal"
)

func TestQueryProxy_Execute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
	qp, err := gproxy.NewQueryProxy(gwal.New())
	if err != nil {
		t.Error(err)
	}

	go gproxy.StartProxy(ctx, qp)
	t.Cleanup(func() {
		gproxy.StopProxy(ctx, qp)
		cancel()
	})

	query, done := gactors.NewLoadFromFileQuery([]byte("default"), []byte("i.csv"))
	go qp.Execute(ctx, query)

	result := <-done
	if !result.Success {
		t.Error("not ok")
	}

	query, done = gactors.NewPrintQuery([]byte("default"))
	go qp.Execute(ctx, query)
	<-done
}

func BenchmarkNewQueryProxy(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
	qp, err := gproxy.NewQueryProxy(gwal.New())
	if err != nil {
		b.Error(err)
	}
	go gproxy.StartProxy(ctx, qp)
	b.Cleanup(func() {
		gproxy.StopProxy(ctx, qp)
		cancel()
	})
	b.Run("execute", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			query, done := gactors.NewLoadFromFileQuery([]byte("default"), []byte("i.csv"))
			go qp.Execute(ctx, query)
			result := <-done
			if !result.Success {
				b.Error("not ok")
			}
		}
	})
}
