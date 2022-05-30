package file

import (
	"encoding/csv"
	"io"
	"os"

	"github.com/blong14/gache/internal/actors"
	gerrors "github.com/blong14/gache/internal/errors"
)

func ReadCSV(data string) ([]actors.KeyValue, error) {
	f, err := os.Open(data)
	if err != nil {
		return nil, gerrors.NewGError(err)
	}
	defer func() { _ = f.Close() }()
	csvReader := csv.NewReader(f)
	out := make([]actors.KeyValue, 0)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, gerrors.NewGError(err)
		}
		out = append(out, actors.KeyValue{Key: []byte(row[0]), Value: []byte(row[1])})
	}
	return out, nil
}

func WriteCSV(data string, keyValues []actors.KeyValue) error {
	f, err := os.Create(data)
	if err != nil {
		return gerrors.NewGError(err)
	}
	defer func() { _ = f.Close() }()
	w := csv.NewWriter(f)
	for _, kv := range keyValues {
		if err := w.Write([]string{string(kv.Key), string(kv.Value)}); err != nil {
			return gerrors.NewGError(err)
		}
	}
	w.Flush()
	return nil
}
