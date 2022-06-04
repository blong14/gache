package main

import (
	"context"
	"encoding/csv"
	"fmt"
	gproxy "github.com/blong14/gache/internal/actors/proxy"
	gwal "github.com/blong14/gache/internal/actors/wal"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"

	gactors "github.com/blong14/gache/internal/actors"
	grepl "github.com/blong14/gache/internal/actors/replication"
	gerrors "github.com/blong14/gache/internal/errors"
	grpc "github.com/blong14/gache/internal/io/rpc"
)

var done chan struct{}

const (
	service     = "gctl"
	environment = "production"
	id          = 1
)

func tracerProvider(url string) (*tracesdk.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exp),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
			attribute.String("environment", environment),
			attribute.Int64("ID", id),
		)),
	)
	return tp, nil
}

func main() {
	if err := os.Setenv("DEBUG", "false"); err != nil {
		log.Fatal(err)
	}
	if err := os.Setenv("TRACE", "false"); err != nil {
		log.Fatal(err)
	}
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	tp, err := tracerProvider("http://jaeger.cluster/api/traces")
	if err != nil {
		log.Fatal(err)
	}
	otel.SetTracerProvider(tp)

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
	errs := gerrors.Append(tp.ForceFlush(ctx), tp.Shutdown(ctx))
	if errs.ErrorOrNil() != nil {
		log.Println(errs)
	}
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
			query, finished := toQuery(ctx, cmd)
			if query == nil || finished == nil {
				continue
			}
			start := time.Now()
			qp.Execute(ctx, query)
			for result := range finished {
				fmt.Println("% --\tkey\tvalue")
				if result.Success {
					fmt.Printf("[%s] 1.\t%s\t%s", time.Since(start), string(result.Key), result.Value)
				}
				break
			}
			close(finished)
		}
	}
}

func toQuery(ctx context.Context, tokens []string) (*gactors.Query, chan gactors.QueryResponse) {
	cmd := tokens[0]
	switch cmd {
	case "exit":
		close(done)
		return nil, nil
	case "get":
		key := tokens[1]
		return gactors.NewGetValueQuery(ctx, []byte("default"), []byte(key))
	case "load":
		data := tokens[1]
		return gactors.NewLoadFromFileQuery(ctx, []byte("default"), []byte(data))
	case "range":
		return gactors.NewRangeQuery(ctx, []byte("default"))
	case "set":
		key := tokens[1]
		value := tokens[2]
		return gactors.NewSetValueQuery(ctx, []byte("default"), []byte(key), []byte(value))
	default:
		return nil, nil
	}
}
