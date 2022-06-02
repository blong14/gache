package proxy_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gproxy "github.com/blong14/gache/internal/actors/proxy"
	gwal "github.com/blong14/gache/internal/actors/wal"
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

func Benchmark_NewQueryProxy(b *testing.B) {
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
				query, done := gactors.NewLoadFromFileQuery(ctx, []byte("default"), []byte("j.csv"))
				qp.Execute(ctx, query)
				result := <-done
				if !result.GetResponse().Success {
					b.Error("not ok")
				}
				close(done)
			}
		})
	})
}

func BenchmarkConcurrent_QueryProxy(b *testing.B) {
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

	readFrac := float32(3) / 10.0
	b.Run("skiplist", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var count int
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pb.Next() {
				key := strconv.Itoa(rng.Intn(100))
				var done chan *gactors.Query
				var query *gactors.Query
				if rng.Float32() < readFrac {
					query, done = gactors.NewGetValueQuery(ctx, []byte("skiplist"), []byte(key))
				} else {
					query, done = gactors.NewSetValueQuery(ctx, []byte("skiplist"), []byte(key), []byte(""))
				}
				qp.Execute(ctx, query)
				result, ok := <-done
				if ok && result.GetResponse().Success {
					count++
				}
				close(done)
			}
		})
		b.ReportMetric(float64(count), "cnt")
	})

	b.Run("treemap", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var count int
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pb.Next() {
				key := strconv.Itoa(rng.Intn(100))
				var done chan *gactors.Query
				var query *gactors.Query
				if rng.Float32() < readFrac {
					query, done = gactors.NewGetValueQuery(ctx, []byte("treemap"), []byte(key))
				} else {
					query, done = gactors.NewSetValueQuery(ctx, []byte("treemap"), []byte(key), []byte(""))
				}
				qp.Execute(ctx, query)
				result, ok := <-done
				if ok && result.GetResponse().Success {
					count++
				}
				close(done)
			}
		})
		b.ReportMetric(float64(count), "cnt")
	})
}
