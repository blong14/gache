package file_test

import (
	"testing"

	gfile "github.com/blong14/gache/internal/io/file"
)

func TestReadJSON(t *testing.T) {
	data, err := gfile.ReadJSON("i.json")
	if err != nil {
		t.Error(err)
	}
	if len(data) == 0 {
		t.Error("value is nil")
	}
	if err := gfile.WriteCSV("i.csv", data); err != nil {
		t.Error(err)
	}
}

func BenchmarkReadJSON(b *testing.B) {
	b.ReportAllocs()
	var l float64
	for i := 0; i < b.N; i++ {
		data, err := gfile.ReadJSON("i.json")
		if err != nil {
			b.Error(err)
		}
		l = float64(len(data))
	}
	b.ReportMetric(l, "items")
}
