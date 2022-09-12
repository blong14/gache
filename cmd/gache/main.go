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
	ghttp "github.com/blong14/gache/internal/io/http"
	grpc "github.com/blong14/gache/internal/io/rpc"
	ghandlers "github.com/blong14/gache/internal/server"
)

func mustGetDB() *sql.DB {
	dsn, ok := os.LookupEnv("dsn")
	if !ok {
		dsn = gache.MEMORY
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

	rpcSRV := ghandlers.Server(":8080")
	go grpc.Start(rpcSRV, ghandlers.RpcHandlers())

	db := mustGetDB()
	httpSRV := ghandlers.Server(":8081")
	go ghttp.Start(httpSRV, ghandlers.HttpHandlers(db))

	s := <-sigint
	log.Printf("received %s signal\n", s)
	ghttp.Stop(ctx, httpSRV, rpcSRV)
	if err := db.Close(); err != nil {
		log.Print(err)
	}
	cancel()
	time.Sleep(500 * time.Millisecond)
}
