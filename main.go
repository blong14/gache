package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	ghttp "github.com/blong14/gache/internal/io/http"
	grpc "github.com/blong14/gache/internal/io/rpc"
)

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	qp, err := NewQueryProxy()
	if err != nil {
		log.Fatal(err)
	}
	go StartProxy(ctx, qp)

	rpcSRV := ghttp.Server(":8080")
	go grpc.Start(rpcSRV, RpcHandlers(qp))

	httpSRV := ghttp.Server(":8081")
	go ghttp.Start(httpSRV, HttpHandlers(qp))

	go RunClient()

	s := <-sigint
	log.Printf("received %s signal\n", s)
	ghttp.Stop(ctx, httpSRV, rpcSRV)
	StopProxy(ctx, qp)
	cancel()
}

func RunClient() {
	client, err := grpc.Client("localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	name := "spoke-01"

	resp, err := Register(client, Spoke{Name: name})
	log.Println(resp, err)

	resp_, _ := SetStatus(client, Spoke{Name: name, Status: "Not OK"})
	log.Println(resp_, err)

	respx, _ := List(client)
	log.Println(respx, err)
}
