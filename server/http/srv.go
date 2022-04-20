package http

import (
	"context"
	"log"
	"net/http"
	"time"
)

type Handler map[string]http.HandlerFunc

func Server(port string) *http.Server {
	return &http.Server{Addr: port}
}

func Start(srv *http.Server, routes Handler) {
	for pattern, handler := range routes {
		http.HandleFunc(pattern, handler)
	}
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Println(err)
	}
}

func Stop(ctx context.Context, srvs ...*http.Server) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for _, srv := range srvs {
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}
}
