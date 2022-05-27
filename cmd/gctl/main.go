package main

import (
	"context"
	"encoding/csv"
	"fmt"
	grepl "github.com/blong14/gache/internal/actors/replication"
	gproxy "github.com/blong14/gache/internal/proxy"
	gwal "github.com/blong14/gache/internal/wal"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	grpc "github.com/blong14/gache/internal/io/rpc"
)

var done chan struct{}

func main() {
	if err := os.Setenv("DEBUG", "false"); err != nil {
		log.Fatal(err)
	}
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	client, err := grpc.Client("localhost:8080")
	if err != nil {
		log.Println(err)
	}

	wal := gwal.New(
		grepl.New(client),
	)
	qp, err := gproxy.NewQueryProxy(wal)
	if err != nil {
		log.Fatal(err)
	}

	gproxy.StartProxy(ctx, qp)

	done = make(chan struct{})
	go Accept(ctx, qp)

	var s os.Signal
	select {
	case sig, ok := <-sigint:
		if ok {
			s = sig
		}
	case <-done:
	}
	log.Printf("received %s signal\n", s)
	gproxy.StopProxy(ctx, qp)
	cancel()
	time.Sleep(500 * time.Millisecond)
}

func Accept(ctx context.Context, qp *gproxy.QueryProxy) {
	time.Sleep(100 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		default:
			fmt.Print("\n% ")
			reader := csv.NewReader(os.Stdin)
			reader.Comma = ' '
			cmd, err := reader.Read()
			if err != nil {
				log.Fatal(err)
			}
			query, finished := toQuery(cmd)
			if query == nil || finished == nil {
				continue
			}
			start := time.Now()
			qp.Execute(ctx, query)
			for result := range finished {
				fmt.Println("% --\tkey\tvalue")
				fmt.Printf("[%s] 1.\t%s\t%s", time.Since(start), string(result.Key), result.Value)
			}
		}
	}
}

func toQuery(tokens []string) (*gactors.Query, <-chan *gactors.QueryResponse) {
	cmd := tokens[0]
	switch cmd {
	case "exit":
		close(done)
		return nil, nil
	case "get":
		key := tokens[1]
		return gactors.NewGetValueQuery([]byte("default"), []byte(key))
	case "load":
		data := tokens[1]
		return gactors.NewLoadFromFileQuery([]byte("default"), []byte(data))
	case "print":
		return gactors.NewPrintQuery([]byte("default"))
	case "range":
		return gactors.NewRangeQuery([]byte("default"))
	case "set":
		key := tokens[1]
		value := tokens[2]
		return gactors.NewSetValueQuery([]byte("default"), []byte(key), []byte(value))
	default:
		return nil, nil
	}
}
