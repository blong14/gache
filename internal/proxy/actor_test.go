package proxy_test

import (
	"context"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gproxy "github.com/blong14/gache/internal/proxy"
	gwal "github.com/blong14/gache/internal/wal"
)

func TestQueryProxy_Execute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	qp, err := gproxy.NewQueryProxy(gwal.New())
	if err != nil {
		t.Error(err)
	}

	gproxy.StartProxy(ctx, qp)
	t.Cleanup(func() {
		gproxy.StopProxy(ctx, qp)
		cancel()
	})

	query, done := gactors.NewLoadFromFileQuery([]byte("default"), []byte("j.csv"))
	qp.Execute(ctx, query)

	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-done:
		if !ok || !result.Success {
			t.Error("not ok")
			return
		}
	}

	query, done = gactors.NewPrintQuery([]byte("default"))
	qp.Execute(ctx, query)

	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-done:
		if !ok || !result.Success {
			t.Error("not ok")
			return
		}
	}
}

func BenchmarkXConcurrent_NewQueryProxy(b *testing.B) {
	b.Setenv("DEBUG", "false")
	b.Setenv("TRACE", "false")
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
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				start := time.Now()
				query, done := gactors.NewLoadFromFileQuery([]byte("default"), []byte("i.csv"))
				qp.Execute(ctx, query)
				result := <-done
				if !result.Success {
					b.Error("not ok")
				}
				b.ReportMetric(float64(time.Since(start).Milliseconds()), "ms")
			}
		})
	})
}
