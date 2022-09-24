package proxy_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gwal "github.com/blong14/gache/internal/actors/wal"
)

func TestQueryProxy_Execute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	qp, err := NewQueryProxy(gwal.New())
	if err != nil {
		t.Error(err)
	}
	StartProxy(ctx, qp)
	t.Cleanup(func() {
		StopProxy(ctx, qp)
		cancel()
	})

	start := time.Now()
	query, done := gactors.NewLoadFromFileQuery(
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
	for _, i := range []int{5} {
		readFrac := float32(i) / 10.0
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
		qp, err := NewQueryProxy(gwal.New())
		if err != nil {
			b.Error(err)
		}
		StartProxy(ctx, qp)

		b.Run(fmt.Sprintf("skiplist_%v", readFrac), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var hits, misses int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					key := strconv.Itoa(rng.Intn(100))
					var query *gactors.Query
					var done chan gactors.QueryResponse
					if rng.Float32() < readFrac {
						query, done = gactors.NewGetValueQuery(ctx, []byte("default"), []byte(key))
					} else {
						query, done = gactors.NewSetValueQuery(ctx, []byte("default"), []byte(key), []byte(""))
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
		StopProxy(ctx, qp)
		cancel()
	}
}

func TestQueryProxy_ExecuteSum(t *testing.T) {
	const max = 10_000_000
	start := time.Now()
	buff := new(bytes.Buffer)
	var rows []gactors.KeyValue
	for i := 0; i < max; i++ {
		err := binary.Write(buff, binary.BigEndian, uint64(i))
		if err != nil {
			t.Error(err)
			return
		}
		value := buff.Bytes()
		rows = append(rows, gactors.KeyValue{Key: value, Value: value})
		buff.Reset()
	}
	t.Logf("finished creating records - %s", time.Since(start))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	qp, err := NewQueryProxy(gwal.New())
	if err != nil {
		t.Error(err)
	}
	StartProxy(ctx, qp)
	t.Cleanup(func() {
		StopProxy(ctx, qp)
		cancel()
	})
	start = time.Now()
	query, done := gactors.NewBatchSetValueQuery(ctx, []byte("default"), rows)
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
	t.Logf("finished loading records - %s", time.Since(start))
	start = time.Now()
	done = make(chan gactors.QueryResponse, 0)
	query = gactors.NewQuery(ctx, done)
	query.Header.Inst = gactors.SumValues
	query.Header.TableName = []byte("default")
	qp.Send(ctx, query)
	select {
	case <-ctx.Done():
		t.Error(ctx.Err())
	case result, ok := <-done:
		if !ok || !result.Success {
			t.Error("not ok")
			return
		}
		t.Logf("finished - %s %v", time.Since(start), result)
	}
}
