package server

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"sync"

	genc "github.com/blong14/gache/server/encoding"
	ghttp "github.com/blong14/gache/server/http"
	grpc "github.com/blong14/gache/server/rpc"
)

func HealthzService(w http.ResponseWriter, _ *http.Request) {
	if _, err := io.WriteString(w, "ok"); err != nil {
		log.Println(err)
	}
}

func HttpHandlers() ghttp.Handler {
	return map[string]http.HandlerFunc{
		"/healthz": HealthzService,
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
	Proxy *QueryProxy
}

type RegisterRequest struct {
	Item Spoke
}

type RegisterResponse struct {
	Status string
	Item   Spoke
}

func (r *RegisterService) Register(req *RegisterRequest, resp *RegisterResponse) error {
	enc := genc.New()
	key := enc.Encode(req.Item.Name)
	value := enc.Encode(req.Item)
	if enc.HasError() {
		return enc.Err
	}
	resp.Status = "not ok"
	if ok := r.Proxy.Set(context.Background(), key, value); ok {
		resp.Status = "ok"
	}
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
	r.Proxy.Range(func(k any, v any) bool {
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

func RpcHandlers(proxy *QueryProxy) []grpc.Handler {
	return []grpc.Handler{
		&RegisterService{
			Proxy: proxy,
		},
		&StatusService{},
	}
}
