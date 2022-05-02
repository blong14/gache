package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	gio "github.com/blong14/gache/internal/io"
	ghttp "github.com/blong14/gache/internal/io/http"
	grpc "github.com/blong14/gache/internal/io/rpc"
	gproxy "github.com/blong14/gache/proxy"
)

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	qp, err := gproxy.NewQueryProxy()
	if err != nil {
		log.Fatal(err)
	}
	go gproxy.StartProxy(ctx, qp)

	rpcSRV := ghttp.Server(":8080")
	go grpc.Start(rpcSRV, gio.RpcHandlers(qp))

	httpSRV := ghttp.Server(":8081")
	go ghttp.Start(httpSRV, gio.HttpHandlers(qp))

	go RunClient()

	s := <-sigint
	log.Printf("received %s signal\n", s)
	ghttp.Stop(ctx, httpSRV, rpcSRV)
	gproxy.StopProxy(ctx, qp)
	cancel()
}

func RunClient() {
	client, err := grpc.Client("localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	name := "spoke-01"
	if _, err = gio.Register(client, gio.Spoke{Name: name}); err != nil {
		log.Println(err)
	}
	if _, err = gio.SetStatus(client, gio.Spoke{Name: name, Status: "Not OK"}); err != nil {
		log.Println(err)
	}
	if _, err = gio.List(client); err != nil {
		log.Println(err)
	}
}
