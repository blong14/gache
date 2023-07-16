package server

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"

	gdb "github.com/blong14/gache/internal/db"
	ghttp "github.com/blong14/gache/internal/io/http"
	gproxy "github.com/blong14/gache/internal/proxy"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

func HealthzService(w http.ResponseWriter, _ *http.Request) {
	if _, err := io.WriteString(w, "ok"); err != nil {
		log.Println(err)
	}
}

type GetValueResponse struct {
	Status string `json:"status"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func getValueService(proxy *gproxy.QueryProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		urlQuery := r.URL.Query()
		if !urlQuery.Has("key") {
			err := ErrorResponse{Error: "missing key"}
			ghttp.MustWriteJSON(w, r, http.StatusBadRequest, err)
			return
		}
		key := urlQuery.Get("key")
		table := urlQuery.Get("table")
		if table == "" {
			table = "default"
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		query, _ := gdb.NewGetValueQuery(ctx, []byte(table), []byte(key))
		proxy.Send(ctx, query)
		result := query.GetResponse()
		var resp GetValueResponse
		var status int
		switch {
		case !result.Success:
			status = http.StatusNotFound
			resp.Status = "not found"
			resp.Key = key
		default:
			status = http.StatusOK
			resp.Status = "ok"
			resp.Key = key
			resp.Value = string(result.Value)
		}
		ghttp.MustWriteJSON(w, r, status, resp)
	}
}

type SetValueRequest struct {
	Table string `json:"table"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type SetValueResponse struct {
	Status string `json:"status"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func setValueService(proxy *gproxy.QueryProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := r.Body
		if body == nil {
			resp := ErrorResponse{Error: "server error"}
			ghttp.MustWriteJSON(w, r, http.StatusInternalServerError, resp)
			return
		}
		defer func() { _ = body.Close() }()
		decoder := json.NewDecoder(body)
		var req SetValueRequest
		if err := decoder.Decode(&req); err != nil {
			resp := ErrorResponse{Error: err.Error()}
			ghttp.MustWriteJSON(w, r, http.StatusUnprocessableEntity, resp)
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		query, _ := gdb.NewSetValueQuery(ctx, []byte(req.Table), []byte(req.Key), []byte(req.Value))
		proxy.Send(ctx, query)
		result := query.GetResponse()
		var resp SetValueResponse
		var status int
		switch {
		case !result.Success:
			status = http.StatusNotFound
			resp.Status = "not found"
			resp.Key = req.Key
		default:
			status = http.StatusCreated
			resp.Status = "created"
			resp.Key = req.Key
			resp.Value = string(result.Value)
		}
		ghttp.MustWriteJSON(w, r, status, resp)
	}
}

func HTTPHandlers(proxy *gproxy.QueryProxy) ghttp.Handler {
	return map[string]http.HandlerFunc{
		"/healthz": HealthzService,
		"/get":     MustBe(http.MethodGet, getValueService(proxy)),
		"/set":     MustBe(http.MethodPost, setValueService(proxy)),
	}
}
