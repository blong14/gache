package file

import (
	"encoding/csv"
	"io"
	"os"

	gerrors "github.com/blong14/gache/internal/errors"
)

func ReadCSV(data string) ([]KeyValue, error) {
	f, err := os.Open(data)
	if err != nil {
		return nil, gerrors.NewGError(err)
	}
	defer func() { _ = f.Close() }()
	csvReader := csv.NewReader(f)
	out := make([]KeyValue, 0)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, gerrors.NewGError(err)
		}
		out = append(out, KeyValue{Key: []byte(row[0]), Value: []byte(row[1])})
	}
	return out, nil
}

func WriteCSV(data string, keyValues []KeyValue) error {
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
