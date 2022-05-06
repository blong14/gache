package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
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

	wal := gwal.New(
		gactors.NewMetricsSubscriber(),
		gproxy.NewQuerySubscriber(nil),
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
	reader := csv.NewReader(os.Stdin)
	reader.Comma = ' '
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		default:
			fmt.Print("\n% ")
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
				fmt.Print("% 1.\t", result.Key, result.Value)
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
	case "set":
		key := tokens[1]
		value := tokens[2]
		return gactors.NewSetValueQuery([]byte("default"), []byte(key), []byte(value))
	}
	return nil, nil
}
