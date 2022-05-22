package file_test

import (
	"testing"

	gfile "github.com/blong14/gache/internal/io/file"
)

func TestReadCSV(t *testing.T) {
	t.Parallel()
	data, err := gfile.ReadCSV("i.csv")
	if err != nil {
		t.Error(err)
	}
	if len(data) == 0 {
		t.Error("value is nil")
	}
}

func BenchmarkReadCSV(b *testing.B) {
	b.ReportAllocs()
	var l float64
	for i := 0; i < b.N; i++ {
		data, err := gfile.ReadCSV("i.csv")
		if err != nil {
			b.Error(err)
		}
		l = float64(len(data))
	}
	b.ReportMetric(l, "items")
}
