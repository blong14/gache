package http

import (
	"encoding/json"
	"log"

	stdhttp "net/http"

	glog "github.com/blong14/gache/internal/logging"
)

func MustWriteJSON(w stdhttp.ResponseWriter, r *stdhttp.Request, status int, resp interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if status >= stdhttp.StatusBadRequest {
		w.WriteHeader(status)
	}
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("Error happened in JSON marshal. Err: %s", err)
	}
	if _, err := w.Write(jsonResp); err != nil {
		log.Fatalf("Error happened in writing JSON. Err: %s", err)
	}
	glog.Track("method=%s status=%v", r.Method, status)
}
