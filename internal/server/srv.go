package server

import "net/http"

func Server(port string) *http.Server {
	return &http.Server{Addr: port}
}
