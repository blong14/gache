package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	gache "github.com/blong14/gache/database"
	gproxy "github.com/blong14/gache/internal/actors/proxy"
	grpc "github.com/blong14/gache/internal/io/rpc"
	ghttp "github.com/blong14/gache/internal/server"
)

func mustGetDB() *sql.DB {
	dsn, ok := os.LookupEnv("dsn")
	if !ok {
		dsn = "::memory::"
	}
	db, err := sql.Open("gache", dsn)
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}
	return db
}

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	db := mustGetDB()
	qp, err := gache.GetProxy(db)
	if err != nil {
		log.Fatal(err)
	}

	rpcSRV := ghttp.Server(":8080")
	go grpc.Start(rpcSRV, gproxy.RpcHandlers(qp))

	httpSRV := ghttp.Server(":8081")
	go ghttp.Start(httpSRV, ghttp.HttpHandlers(db))

	s := <-sigint
	log.Printf("received %s signal\n", s)
	ghttp.Stop(ctx, httpSRV, rpcSRV)
	cancel()
	time.Sleep(500 * time.Millisecond)
}
