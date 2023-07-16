package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	ghttp "github.com/blong14/gache/internal/io/http"
	grpc "github.com/blong14/gache/internal/io/rpc"
	gproxy "github.com/blong14/gache/internal/proxy"
	ghandlers "github.com/blong14/gache/internal/server"
)

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	proxy, err := gproxy.NewQueryProxy()
	if err != nil {
		panic(err)
	}
	gproxy.StartProxy(ctx, proxy)

	rpcSRV := ghandlers.Server(":8080")
	go grpc.Start(rpcSRV, ghandlers.RPCHandlers())

	httpSRV := ghandlers.Server(":8081")
	go ghttp.Start(httpSRV, ghandlers.HTTPHandlers(proxy))

	s := <-sigint
	log.Printf("received %s signal\n", s)
	ghttp.Stop(ctx, httpSRV, rpcSRV)
	gproxy.StopProxy(ctx, proxy)
	cancel()
	time.Sleep(500 * time.Millisecond)
}
