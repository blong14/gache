package io

import (
	"context"
	"encoding/json"
	glog "github.com/blong14/gache/logging"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	gactors "github.com/blong14/gache/internal/actors"
	genc "github.com/blong14/gache/internal/io/encoding"
	ghttp "github.com/blong14/gache/internal/io/http"
	grpc "github.com/blong14/gache/internal/io/rpc"
	gproxy "github.com/blong14/gache/proxy"
)

func HealthzService(w http.ResponseWriter, _ *http.Request) {
	if _, err := io.WriteString(w, "ok"); err != nil {
		log.Println(err)
	}
}

func GetValueService(qp *gproxy.QueryProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
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
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		go qp.Get(ctx, query)
		if value, ok := query.Result(ctx); !ok {
			resp["error"] = "not found"
			ghttp.MustWriteJSON(w, r, http.StatusNotFound, resp)
		} else {
			resp["status"] = "ok"
			resp["key"] = string(key)
			resp["value"] = string(value)
			ghttp.MustWriteJSON(w, r, http.StatusOK, resp)
			glog.Track("Get key=%s time=%s", query.Key, time.Since(start))
		}
	}
}

type SetValueRequest struct {
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
		query := gactors.NewQuery()
		query.Key = req.Key
		query.Value = req.Value
		go qp.Set(context.TODO(), &query)
		if _, ok := query.Result(context.TODO()); !ok {
			ghttp.MustWriteJSON(w, r, http.StatusUnprocessableEntity, resp)
			return
		}
		resp["status"] = "ok"
		resp["key"] = string(req.Key)
		resp["value"] = string(req.Value)
		ghttp.MustWriteJSON(w, r, http.StatusCreated, resp)
	}
}

func HttpHandlers(qp *gproxy.QueryProxy) ghttp.Handler {
	return map[string]http.HandlerFunc{
		"/healthz": HealthzService,
		"/get":     ghttp.MustBe(http.MethodGet, GetValueService(qp)),
		"/set":     ghttp.MustBe(http.MethodPost, SetValueService(qp)),
	}
}

type Spoke struct {
	Name   string
	Addr   string
	Status string
}

var (
	mtx    = new(sync.RWMutex)
	spokes = map[string]Spoke{}
)

type RegisterService struct {
	Proxy *gproxy.QueryProxy
}

type RegisterRequest struct {
	Item Spoke
}

type RegisterResponse struct {
	Status string
	Item   Spoke
}

func (r *RegisterService) Register(req *RegisterRequest, resp *RegisterResponse) error {
	glog.Track("%T not implemeneted", r)
	return nil
}

func Register(client *rpc.Client, spoke Spoke) (*RegisterResponse, error) {
	req := new(RegisterRequest)
	req.Item = spoke
	resp := new(RegisterResponse)
	err := client.Call("RegisterService.Register", req, resp)
	return resp, err
}

type RegisterListResponse struct {
	Items  []RegisterResponse
	Status string
}

func (r *RegisterService) List(_ *RegisterRequest, resp *RegisterListResponse) error {
	enc := genc.New()
	ctx := context.Background()
	r.Proxy.Range(context.TODO(), func(k any, v any) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			var value Spoke
			enc.Decode(v.([]byte), &value)
			if enc.HasError() {
				enc.Reset()
				return true
			}
			resp.Items = append(resp.Items, RegisterResponse{Item: value, Status: value.Status})
			return true
		}
	})
	resp.Status = "ok"
	return nil
}

func List(client *rpc.Client) (*RegisterListResponse, error) {
	req := new(RegisterRequest)
	resp := new(RegisterListResponse)
	err := client.Call("RegisterService.List", req, resp)
	return resp, err
}

type StatusService struct{}

type StatusRequest struct {
	Item Spoke
}

type StatusResponse struct {
	Status string
	Item   Spoke
}

func (s *StatusService) SetStatus(req *StatusRequest, resp *StatusResponse) error {
	mtx.Lock()
	if spoke, ok := spokes[req.Item.Name]; ok {
		spoke.Status = req.Item.Status
	}
	mtx.Unlock()
	resp.Status = "ok"
	return nil
}

func SetStatus(client *rpc.Client, spoke Spoke) (*StatusResponse, error) {
	req := new(StatusRequest)
	req.Item = spoke
	resp := new(StatusResponse)
	err := client.Call("StatusService.SetStatus", req, resp)
	return resp, err
}

func RpcHandlers(proxy *gproxy.QueryProxy) []grpc.Handler {
	return []grpc.Handler{
		&RegisterService{
			Proxy: proxy,
		},
		&StatusService{},
	}
}
