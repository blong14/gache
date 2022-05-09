package json_test

import (
	"testing"

	gjson "github.com/blong14/gache/internal/io/json"
)

func TestReadJSON(t *testing.T) {
	data, err := gjson.ReadJSON("i.json")
	if err != nil {
		t.Error(err)
	}
	if len(data) == 0 {
		t.Error("value is nil")
	}
}

func BenchmarkReadJSON(b *testing.B) {
	b.ReportAllocs()
	var l float64
	for i := 0; i < b.N; i++ {
		data, err := gjson.ReadJSON("i.json")
		if err != nil {
			b.Error(err)
		}
		l = float64(len(data))
	}
	b.ReportMetric(l, "items")
}
