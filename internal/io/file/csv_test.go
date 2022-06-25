package file_test

import (
	"github.com/blong14/gache/internal/actors"
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
	out := make([]actors.KeyValue, 0)
	for i := 0; i < b.N; i++ {
		data, err := gfile.ReadCSV("i.csv")
		if err != nil {
			b.Error(err)
		}
		out = append(out, data...)
	}
	b.ReportMetric(float64(len(out)), "items")
}

func BenchmarkScanCSV(b *testing.B) {
	b.ReportAllocs()
	out := make([]actors.KeyValue, 0)
	for i := 0; i < b.N; i++ {
		reader := gfile.ScanCSV("i.csv")
		reader.Init()
		for reader.Scan() {
			out = append(out, reader.Rows()...)
		}
	}
	b.ReportMetric(float64(len(out)), "items")
}
