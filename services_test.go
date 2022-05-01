package main_test

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	gsrv "github.com/blong14/gache"
)

func assertResponse(
	t *testing.T,
	w http.ResponseWriter,
	r *http.Request,
	handler http.HandlerFunc,
	fnc func(w http.ResponseWriter) bool,
) {
	t.Helper()
	handler(w, r)
	if ok := fnc(w); !ok {
		t.Error("http error")
	}
}

func getValueServiceTest(t *testing.T) {
	t.Parallel()
	qp, err := gsrv.NewQueryProxy()
	if err != nil {
		t.Error(err)
	}
	handler := gsrv.GetValueService(qp)
	values := &url.Values{}
	values.Add("key", "1")
	req, _ := http.NewRequest(http.MethodGet, "/get", nil)
	req.URL.RawQuery = values.Encode()
	req = req.Clone(context.TODO())
	w := &httptest.ResponseRecorder{}
	assertResponse(t, w, req, handler, func(w http.ResponseWriter) bool {
		resp, ok := w.(*httptest.ResponseRecorder)
		if !ok {
			return false
		}
		r := resp.Result()
		if r.StatusCode != http.StatusOK {
			return false
		}
		value, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return false
		}
		log.Println(value)
		return true
	})
}

func setStatusTest(t *testing.T) {
	t.Parallel()
	t.Skip("not implemented")
}

func registerListTest(srv *gsrv.RegisterService) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()

		// given
		req := &gsrv.RegisterRequest{}
		expected := &gsrv.RegisterListResponse{
			Items:  []gsrv.RegisterResponse{},
			Status: "ok",
		}

		// when
		actual := &gsrv.RegisterListResponse{}
		if err := srv.List(req, actual); err != nil {
			t.Error(err)
		}

		// then
		if actual.Status != "ok" {
			t.Errorf("\nwant %v\n got %v", "ok", actual.Status)
		}
		if len(expected.Items) != len(actual.Items) {
			t.Errorf("\nwant %v\n got %v", expected, actual)
		}
	}
}

func TestServices(t *testing.T) {
	t.Parallel()
	qp, err := gsrv.NewQueryProxy()
	if err != nil {
		t.Fail()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	go gsrv.StartProxy(ctx, qp)
	service := &gsrv.RegisterService{
		Proxy: qp,
	}
	t.Run("should get value", getValueServiceTest)
	t.Run("should list all spokes", registerListTest(service))
	t.Run("should set spoke status", setStatusTest)
	t.Cleanup(func() {
		gsrv.StopProxy(ctx, qp)
		cancel()
	})
}
