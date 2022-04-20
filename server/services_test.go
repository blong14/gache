package server_test

import (
	"testing"

	gsrv "github.com/blong14/gache/server"
)

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
	service := &gsrv.RegisterService{
		Proxy: qp,
	}
	t.Run("should list all spokes", registerListTest(service))
	t.Run("should set spoke status", setStatusTest)
}
