package file_test

import (
	"path/filepath"
	"testing"

	gfile "github.com/blong14/gache/internal/io/file"
)

func TestReadCSV(t *testing.T) {
	t.Parallel()
	scanner := gfile.ScanCSV(filepath.Join("testdata", "i.csv"))
	scanner.Init()
	var count int
	for scanner.Scan() {
		count += len(scanner.Rows())
	}
	if count == 0 {
		t.Error("value is nil")
	}
	t.Log(count)
}

func BenchmarkScanCSV(b *testing.B) {
	b.ReportAllocs()
	out := make([][]string, 0)
	for i := 0; i < b.N; i++ {
		reader := gfile.ScanCSV(filepath.Join("testdata", "i.csv"))
		reader.Init()
		for reader.Scan() {
			out = append(out, reader.Rows()...)
		}
	}
	b.ReportMetric(float64(len(out)), "items")
}
