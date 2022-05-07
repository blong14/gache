package main

import (
	"context"
	"encoding/csv"
	"fmt"
	grepl "github.com/blong14/gache/proxy/replication"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	gmetrics "github.com/blong14/gache/internal/actors/metrics"
	grpc "github.com/blong14/gache/internal/io/rpc"
	gproxy "github.com/blong14/gache/proxy"
	gwal "github.com/blong14/gache/proxy/wal"
)

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
		gmetrics.New(),
		grepl.New(client),
	)
	qp, err := gproxy.NewQueryProxy(wal)
	if err != nil {
		log.Fatal(err)
	}

	gproxy.StartProxy(ctx, qp)

	go Accept(ctx, qp)

	s := <-sigint
	log.Printf("received %s signal\n", s)
	gproxy.StopProxy(ctx, qp)
	cancel()
	time.Sleep(500 * time.Millisecond)
}

var done chan struct{}

func Accept(ctx context.Context, qp *gproxy.QueryProxy) {
	done = make(chan struct{})
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
			query, done := toQuery(cmd)
			if query == nil || done == nil {
				return
			}
			go qp.Execute(ctx, query)
			for result := range done {
				fmt.Println("% --\tkey\tvalue")
				fmt.Printf("- 1.\t%s\t%v", string(result.Key), result.Value)
			}
		}
	}
}

func toQuery(tokens []string) (*gactors.Query, chan *gactors.QueryResponse) {
	cmd := tokens[0]
	switch cmd {
	case "exit":
		close(done)
	case "get":
		key := tokens[1]
		return gactors.NewGetValueQuery([]byte("default"), []byte(key))
	case "load":
		data := tokens[1]
		return gactors.NewLoadFromFileQuery([]byte("default"), []byte(data))
	case "print":
		return gactors.NewPrintQuery([]byte("default"))
	case "set":
		key := tokens[1]
		value := tokens[2]
		return gactors.NewSetValueQuery([]byte("default"), []byte(key), []byte(value))
	}
	return nil, nil
}
