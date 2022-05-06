package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gio "github.com/blong14/gache/internal/io"
	ghttp "github.com/blong14/gache/internal/io/http"
	grpc "github.com/blong14/gache/internal/io/rpc"
	gproxy "github.com/blong14/gache/proxy"
	gwal "github.com/blong14/gache/proxy/wal"
)

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	wal := gwal.New(
		gactors.NewMetricsSubscriber(),
		gproxy.NewQuerySubscriber(nil),
	)
	qp, err := gproxy.NewQueryProxy(wal)
	if err != nil {
		log.Fatal(err)
	}

	rpcSRV := ghttp.Server(":8080")
	go grpc.Start(rpcSRV, gproxy.RpcHandlers(qp))

	httpSRV := ghttp.Server(":8081")
	go ghttp.Start(httpSRV, gio.HttpHandlers(qp))

	gproxy.StartProxy(ctx, qp)

	s := <-sigint
	log.Printf("received %s signal\n", s)
	ghttp.Stop(ctx, httpSRV, rpcSRV)
	gproxy.StopProxy(ctx, qp)
	cancel()
	time.Sleep(500 * time.Millisecond)
}
