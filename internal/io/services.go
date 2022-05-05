package io

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"

	gactors "github.com/blong14/gache/internal/actors"
	ghttp "github.com/blong14/gache/internal/io/http"
	gproxy "github.com/blong14/gache/proxy"
)

func HealthzService(w http.ResponseWriter, _ *http.Request) {
	if _, err := io.WriteString(w, "ok"); err != nil {
		log.Println(err)
	}
}

func GetValueService(qp *gproxy.QueryProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := make(map[string]string)
		urlQuery := r.URL.Query()
		if !urlQuery.Has("key") {
			ghttp.MustWriteJSON(w, r, http.StatusBadRequest, resp)
			return
		}
		database := urlQuery.Get("database")
		if database == "" {
			database = "default"
		}
		db := []byte(database)
		key := []byte(urlQuery.Get("key"))
		query := gactors.NewGetValueQuery(db, key)
		defer query.Finish()
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		go qp.Execute(ctx, query)
		if value, ok := query.Result(ctx); !ok {
			resp["error"] = "not found"
			ghttp.MustWriteJSON(w, r, http.StatusNotFound, resp)
		} else {
			resp["status"] = "ok"
			resp["key"] = string(key)
			resp["value"] = string(value)
			ghttp.MustWriteJSON(w, r, http.StatusOK, resp)
		}
	}
}

type SetValueRequest struct {
	Table []byte `json:"table"`
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

func SetValueService(qp *gproxy.QueryProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := make(map[string]string)
		body, err := r.GetBody()
		if err != nil {
			ghttp.MustWriteJSON(w, r, http.StatusInternalServerError, resp)
			return
		}
		defer func() { _ = body.Close() }()
		decoder := json.NewDecoder(body)
		var req SetValueRequest
		if err := decoder.Decode(&req); err != nil {
			resp["error"] = err.Error()
			ghttp.MustWriteJSON(w, r, http.StatusBadRequest, resp)
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		query := gactors.NewSetValueQuery(req.Table, req.Key, req.Value)
		go qp.Execute(ctx, query)
		if _, ok := query.Result(ctx); !ok {
			ghttp.MustWriteJSON(w, r, http.StatusUnprocessableEntity, resp)
		} else {
			resp["status"] = "ok"
			resp["key"] = string(req.Key)
			resp["value"] = string(req.Value)
			ghttp.MustWriteJSON(w, r, http.StatusCreated, resp)
		}
	}
}

func HttpHandlers(qp *gproxy.QueryProxy) ghttp.Handler {
	return map[string]http.HandlerFunc{
		"/healthz": HealthzService,
		"/get":     ghttp.MustBe(http.MethodGet, GetValueService(qp)),
		"/set":     ghttp.MustBe(http.MethodPost, SetValueService(qp)),
	}
}
