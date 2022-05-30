package main

import (
	"context"
	proxy2 "github.com/blong14/gache/internal/actors/proxy"
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

	gmetrics "github.com/blong14/gache/internal/actors/metrics"
	grepl "github.com/blong14/gache/internal/actors/replication"
	gerrors "github.com/blong14/gache/internal/errors"
	gio "github.com/blong14/gache/internal/io"
	ghttp "github.com/blong14/gache/internal/io/http"
	grpc "github.com/blong14/gache/internal/io/rpc"
)

const (
	service     = "gache"
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
		gmetrics.New(),
		grepl.New(client),
	)
	qp, err := proxy2.NewQueryProxy(wal)
	if err != nil {
		log.Fatal(err)
	}

	rpcSRV := ghttp.Server(":8080")
	go grpc.Start(rpcSRV, proxy2.RpcHandlers(qp))

	httpSRV := ghttp.Server(":8081")
	go ghttp.Start(httpSRV, gio.HttpHandlers(qp))

	proxy2.StartProxy(ctx, qp)

	s := <-sigint
	log.Printf("received %s signal\n", s)
	ghttp.Stop(ctx, httpSRV, rpcSRV)
	proxy2.StopProxy(ctx, qp)
	errs := gerrors.Append(tp.ForceFlush(ctx), tp.Shutdown(ctx))
	if errs.ErrorOrNil() != nil {
		log.Println(errs)
	}
	cancel()
	time.Sleep(500 * time.Millisecond)
}
