package file

import (
	"encoding/csv"
	"io"
	"math"
	"os"
	"runtime"

	gerrors "github.com/blong14/gache/internal/errors"
)

type Reader struct {
	max       int
	token     [][]string
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
	s.csvReader.ReuseRecord = false
	s.token = make([][]string, s.max)
}

func (s *Reader) Err() *gerrors.Error {
	return s.errs
}

func (s *Reader) Rows() [][]string {
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
	out := make([][]string, 0, s.max)
	for {
		row, err := s.csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.errs = gerrors.Append(s.errs, err)
			break
		}
		out = append(out, row)
		if len(out) == s.max {
			copy(s.token, out)
			return true
		}
	}
	if len(out) > 0 {
		s.token = out
		return true
	}
	return false
}

var (
	base     = float64(2)
	exponent = float64(14)
)

func ScanCSV(data string) *Reader {
	return &Reader{
		data: data,
		max:  int(math.Pow(base, exponent)) / (runtime.NumCPU() / int(base)),
	}
}
