package proxy_test

import (
	"context"
	glog "github.com/blong14/gache/logging"
	"testing"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gproxy "github.com/blong14/gache/proxy"
	gwal "github.com/blong14/gache/proxy/wal"
)

func TestQueryProxy_Execute(t *testing.T) {
	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	ctx := context.TODO()
	qp, err := gproxy.NewQueryProxy(gwal.New())
	if err != nil {
		t.Error(err)
	}

	go gproxy.StartProxy(ctx, qp)
	t.Cleanup(func() {
		glog.Track("done")
		// gproxy.StopProxy(ctx, qp)
		// cancel()
	})

	query, done := gactors.NewLoadFromFileQuery([]byte("default"), []byte("i.json"))
	go qp.Execute(ctx, query)

	start := time.Now()
	result := <-done
	if !result.Success {
		t.Error("not ok")
	}
	t.Log(time.Since(start))

	query, done = gactors.NewPrintQuery([]byte("default"))
	go qp.Execute(ctx, query)
	<-done
}
