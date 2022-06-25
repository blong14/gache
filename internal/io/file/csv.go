package file

import (
	"encoding/csv"
	"io"
	"os"
	"runtime"

	"github.com/blong14/gache/internal/actors"
	gerrors "github.com/blong14/gache/internal/errors"
)

type Reader struct {
	max       int
	token     []actors.KeyValue
	errs      *gerrors.Error
	data      string
	handle    *os.File
	csvReader *csv.Reader
}

func (s *Reader) Init() {
	f, err := os.Open(s.data)
	if err != nil {
		panic(err)
	}
	s.handle = f
	s.csvReader = csv.NewReader(s.handle)
	s.csvReader.ReuseRecord = true
	s.token = make([]actors.KeyValue, s.max)
}

func (s *Reader) Err() *gerrors.Error {
	return s.errs
}

func (s *Reader) Rows() []actors.KeyValue {
	return s.token
}

func (s *Reader) Close() {
	if err := s.handle.Close(); err != nil {
		s.errs = gerrors.Append(s.errs, err)
	}
}

func (s *Reader) Scan() bool {
	if err := s.errs.ErrorOrNil(); err != nil {
		return false
	}
	out := make([]actors.KeyValue, 0)
	for {
		row, err := s.csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.errs = gerrors.Append(s.errs, err)
			break
		}
		out = append(out, actors.KeyValue{
			Key:   []byte(row[0]),
			Value: []byte(row[1]),
		})
		if len(out) == s.max {
			copy(s.token, out)
			return true
		}
	}
	if len(out) > 0 {
		copy(s.token, out)
		return true
	}
	return false
}

func ScanCSV(data string) *Reader {
	return &Reader{
		data: data,
		max:  18000 / runtime.NumCPU(),
	}
}

func ReadCSV(data string) ([]actors.KeyValue, error) {
	f, err := os.Open(data)
	if err != nil {
		return nil, gerrors.NewGError(err)
	}
	defer func() { _ = f.Close() }()
	csvReader := csv.NewReader(f)
	csvReader.ReuseRecord = true
	out := make([]actors.KeyValue, 0)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, gerrors.NewGError(err)
		}
		out = append(out, actors.KeyValue{
			Key:   []byte(row[0]),
			Value: []byte(row[1]),
		})
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
		err := w.Write([]string{string(kv.Key), string(kv.Value)})
		if err != nil {
			return gerrors.NewGError(err)
		}
	}
	w.Flush()
	return nil
}
