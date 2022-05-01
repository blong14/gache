package rpc

import (
	"log"
	"net/http"
	"net/rpc"
)

type Handler any

func Start(srv *http.Server, services []Handler) {
	for _, service := range services {
		if err := rpc.Register(service); err != nil {
			log.Fatal(err)
		}
	}
	rpc.HandleHTTP()
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Println(err)
	}
}

func Client(addr string) (*rpc.Client, error) {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, err
	}
	return client, nil
}
