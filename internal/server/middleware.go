package server

import (
	"net/http"

	ghttp "github.com/blong14/gache/internal/platform/io/http"
)

func MustBe(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			err := ErrorResponse{Error: "method not allowed"}
			ghttp.MustWriteJSON(w, r, http.StatusMethodNotAllowed, err)
			return
		}
		next(w, r)
	}
}
