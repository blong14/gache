package proxy_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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
		err = os.Remove(filepath.Join("testdata", "default-wal.dat"))
		if err != nil {
			t.Log(err)
		}
		err = os.Remove(filepath.Join("testdata", "default.dat"))
		if err != nil {
			t.Log(err)
		}
	})

	start := time.Now()
	query, done := gdb.NewLoadFromFileQuery(
		ctx, []byte("default"), []byte(filepath.Join("testdata", "i.csv")))
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

		b.Run(fmt.Sprintf("skiplist_%v", i*10), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			table := []byte("default")
			value := []byte{'v'}
			var hits, misses int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				buf := make([]byte, 8)
				for pb.Next() {
					var query *gdb.Query
					var done chan gdb.QueryResponse
					if rng.Float32() < readFrac {
						query, done = gdb.NewGetValueQuery(ctx, table, randomKey(rng, buf))
					} else {
						query, done = gdb.NewSetValueQuery(ctx, table, randomKey(rng, buf), value)
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

func randomKey(rng *rand.Rand, b []byte) []byte {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}
