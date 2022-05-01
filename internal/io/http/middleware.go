package http

import (
	stdhttp "net/http"
)

func MustBe(method string, next stdhttp.HandlerFunc) stdhttp.HandlerFunc {
	return func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.Method != method {
			MustWriteJSON(w, r, stdhttp.StatusMethodNotAllowed, map[string]string{})
			return
		}
		next(w, r)
	}
}
