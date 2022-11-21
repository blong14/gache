package proxy_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	gdb "github.com/blong14/gache/internal/db"
	gproxy "github.com/blong14/gache/internal/proxy"
)

func TestQueryProxy_Execute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	qp, err := gproxy.NewQueryProxy()
	if err != nil {
		t.Error(err)
	}
	gproxy.StartProxy(ctx, qp)
	t.Cleanup(func() {
		gproxy.StopProxy(ctx, qp)
		cancel()
	})

	start := time.Now()
	query, done := gdb.NewLoadFromFileQuery(
		ctx, []byte("default.dat"), []byte(filepath.Join("testdata", "i.csv")))
	qp.Send(ctx, query)
	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-done:
		if !ok || !result.Success {
			t.Error("not ok")
			return
		}
	}
	t.Logf("finished - %s", time.Since(start))
}

func BenchmarkConcurrent_QueryProxy(b *testing.B) {
	b.Setenv("DEBUG", "false")
	b.Setenv("TRACE", "false")
	for _, i := range []int{3, 5, 7} {
		readFrac := float32(i) / 10.0
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
		qp, err := gproxy.NewQueryProxy()
		if err != nil {
			b.Error(err)
		}
		gproxy.StartProxy(ctx, qp)

		b.Run(fmt.Sprintf("skiplist_%v", readFrac), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var hits, misses int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := strconv.Itoa(rng.Intn(100))
					var query *gdb.Query
					var done chan gdb.QueryResponse
					if rng.Float32() < readFrac {
						query, done = gdb.NewGetValueQuery(ctx, []byte("default."), []byte(key))
					} else {
						query, done = gdb.NewSetValueQuery(ctx, []byte("default."), []byte(key), []byte(key))
					}
					qp.Send(ctx, query)
					select {
					case <-ctx.Done():
						b.Error(ctx.Err())
					case result := <-done:
						if result.Success {
							hits++
						} else {
							misses++
						}
					}
				}
			})
			b.ReportMetric(float64(hits), "hits")
			b.ReportMetric(float64(misses), "misses")
		})
		gproxy.StopProxy(ctx, qp)
		cancel()
	}
}
