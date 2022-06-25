package proxy_test

import (
	"context"
	"fmt"
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
		close(done)
		cancel()
	})

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

	query, finished := gactors.NewPrintQuery(ctx, []byte("default"))
	qp.Execute(ctx, query)

	t.Cleanup(func() {
		close(finished)
	})

	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-finished:
		if !ok || !result.Success {
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
		cancel()
	})
	b.Run("execute", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				query, done := gactors.NewLoadFromFileQuery(
					ctx, []byte("default"), []byte("i.csv"),
				)
				qp.Execute(ctx, query)
				result := <-done
				if !result.Success {
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
		cancel()
	})

	for _, i := range []int{3, 5, 7} {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("skiplist_%v", readFrac), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var hits int
			var misses int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := strconv.Itoa(rng.Intn(100))
					var done chan gactors.QueryResponse
					var query *gactors.Query
					if rng.Float32() < readFrac {
						query, done = gactors.NewGetValueQuery(ctx, []byte("default"), []byte(key))
					} else {
						query, done = gactors.NewSetValueQuery(ctx, []byte("default"), []byte(key), []byte(""))
					}
					qp.Execute(ctx, query)
					result, ok := <-done
					if ok && result.Success {
						hits++
					} else {
						misses++
					}
					close(done)
				}
			})
			b.ReportMetric(float64(hits), "hits")
			b.ReportMetric(float64(misses), "misses")
		})
	}
}
