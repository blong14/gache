package file_test

import (
	"path/filepath"
	"testing"

	gfile "github.com/blong14/gache/internal/io/file"
)

func TestReadDat(t *testing.T) {
	t.Parallel()
	scanner := gfile.ScanDat(filepath.Join("testdata", "i.dat"))
	defer scanner.Close()
	scanner.Init()
	var count int
	for scanner.Scan() {
		count = count + len(scanner.Rows())
	}
	if count == 0 {
		t.Error("value is nil")
	}
	t.Log(count)
}
