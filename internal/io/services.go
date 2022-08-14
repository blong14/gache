package io

import (
	"context"
	"encoding/json"
	gproxy "github.com/blong14/gache/internal/actors/proxy"
	"io"
	"log"
	"net/http"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	ghttp "github.com/blong14/gache/internal/io/http"
	glog "github.com/blong14/gache/internal/logging"
)

func HealthzService(w http.ResponseWriter, _ *http.Request) {
	if _, err := io.WriteString(w, "ok"); err != nil {
		log.Println(err)
	}
}

func GetValueService(qp *gproxy.QueryProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() { glog.Track("get %s", time.Since(start)) }()
		resp := make(map[string]string)
		urlQuery := r.URL.Query()
		if !urlQuery.Has("key") {
			ghttp.MustWriteJSON(w, r, http.StatusBadRequest, resp)
			return
		}
		database := urlQuery.Get("table")
		if database == "" {
			database = "default"
		}

		db := []byte(database)
		key := []byte(urlQuery.Get("key"))

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		query, outbox := gactors.NewGetValueQuery(ctx, db, key)
		qp.Enqueue(ctx, query)

		var status int
		select {
		case <-ctx.Done():
			status = http.StatusInternalServerError
		case result, ok := <-outbox:
			switch {
			case !ok:
				status = http.StatusInternalServerError
			case result.Success:
				resp["status"] = "ok"
				resp["key"] = string(key)
				resp["value"] = string(result.Value)
				status = http.StatusOK
			default:
				status = http.StatusNotFound
			}
		}
		ghttp.MustWriteJSON(w, r, status, resp)
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

		query, outbox := gactors.NewSetValueQuery(ctx, req.Table, req.Key, req.Value)
		qp.Enqueue(ctx, query)

		var status int
		select {
		case <-ctx.Done():
			status = http.StatusInternalServerError
		case result, ok := <-outbox:
			switch {
			case !ok:
				status = http.StatusInternalServerError
			case result.Success:
				resp["status"] = "created"
				resp["key"] = string(req.Key)
				resp["value"] = string(req.Value)
				status = http.StatusCreated
			default:
				status = http.StatusUnprocessableEntity
			}
		}
		ghttp.MustWriteJSON(w, r, status, resp)
	}
}

func HttpHandlers(qp *gproxy.QueryProxy) ghttp.Handler {
	return map[string]http.HandlerFunc{
		"/healthz": HealthzService,
		"/get":     ghttp.MustBe(http.MethodGet, GetValueService(qp)),
		"/set":     ghttp.MustBe(http.MethodPost, SetValueService(qp)),
	}
}
