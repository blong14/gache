package proxy_test

import (
	"context"
	gproxy "github.com/blong14/gache/internal/actors/proxy"
	gwal "github.com/blong14/gache/internal/actors/wal"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
)

func TestQueryProxy_Execute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	qp, err := gproxy.NewQueryProxy(gwal.New())
	if err != nil {
		t.Error(err)
	}

	query, done := gactors.NewLoadFromFileQuery(ctx, []byte("default"), []byte("j.csv"))
	gproxy.StartProxy(ctx, qp)
	t.Cleanup(func() {
		gproxy.StopProxy(ctx, qp)
		close(done)
		cancel()
	})

	qp.Execute(ctx, query)

	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-done:
		if !ok || !result.GetResponse().Success {
			t.Error("not ok")
			return
		}
	}

	query, finished := gactors.NewPrintQuery(ctx, []byte("default"))
	qp.Execute(ctx, query)

	t.Cleanup(func() {
		close(finished)
	})

	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-finished:
		if !ok || !result.GetResponse().Success {
			t.Error("not ok")
			return
		}
	}
}

func BenchmarkConcurrent_NewQueryProxy(b *testing.B) {
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
				query, done := gactors.NewLoadFromFileQuery(ctx, []byte("default"), []byte("i.csv"))
				qp.Execute(ctx, query)
				result := <-done
				if !result.GetResponse().Success {
					b.Error("not ok")
				}
				b.ReportMetric(float64(time.Since(start).Milliseconds()), "ms")
				close(done)
			}
		})
	})
}
