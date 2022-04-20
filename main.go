package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	gsrv "github.com/blong14/gache/server"
	ghttp "github.com/blong14/gache/server/http"
	grpc "github.com/blong14/gache/server/rpc"
)

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	qp, err := gsrv.NewQueryProxy()
	if err != nil {
		log.Fatal(err)
	}
	go qp.Listen(ctx)

	rpcSRV := ghttp.Server(":8080")
	go grpc.Start(rpcSRV, gsrv.RpcHandlers(qp))

	httpSRV := ghttp.Server(":8081")
	go ghttp.Start(httpSRV, gsrv.HttpHandlers())

	go RunClient()

	s := <-sigint
	log.Printf("received %s signal\n", s)
	ghttp.Stop(ctx, httpSRV, rpcSRV)
	cancel()
	gsrv.CloseQueryProxy(qp)
}

func RunClient() {
	client, err := grpc.Client("localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	name := "spoke-01"

	resp, err := gsrv.Register(client, gsrv.Spoke{Name: name})
	log.Println(resp, err)

	resp_, _ := gsrv.SetStatus(client, gsrv.Spoke{Name: name, Status: "Not OK"})
	log.Println(resp_, err)

	respx, _ := gsrv.List(client)
	log.Println(respx, err)
}
