package file

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"math"
	"os"
	"runtime"

	gdb "github.com/blong14/gache/internal/db"
	garena "github.com/blong14/gache/internal/db/arena"
	gerrors "github.com/blong14/gache/internal/errors"
)

type DatReader struct {
	max    int
	token  []gdb.KeyValue
	errs   *gerrors.Error
	data   string
	handle *os.File
	buffer *bytes.Buffer
	memmap garena.Map
	dat    [][]byte
	next   int
}

func (s *DatReader) Init() {
	f, err := os.Open(s.data)
	if err != nil {
		panic(err)
	}
	m, err := garena.NewMap(f, garena.Prot(garena.Read), garena.Flag(garena.Shared))
	if err != nil {
		panic(err)
	}
	s.memmap = m
	s.handle = f
	s.token = make([]gdb.KeyValue, s.max)
	s.dat = bytes.Split(s.memmap.Bytes(), []byte(";"))
	s.next = -1
	s.buffer = bytes.NewBuffer(make([]byte, 0))
}

func (s *DatReader) Err() *gerrors.Error {
	return s.errs
}

func (s *DatReader) Rows() []gdb.KeyValue {
	return s.token
}

func (s *DatReader) Close() {
	if err := s.handle.Close(); err != nil {
		s.errs = gerrors.Append(s.errs, err)
	}
	if err := s.memmap.Close(); err != nil {
		s.errs = gerrors.Append(s.errs, err)
	}
}

func (s *DatReader) Scan() bool {
	if err := s.errs.ErrorOrNil(); err != nil {
		return false
	}
	out := make([]gdb.KeyValue, 0)
	next := s.next + 1
	if next > len(s.dat) {
		return false
	}
	for i, _byts := range s.dat[next:] {
		s.buffer.Reset()
		s.buffer.Write(_byts)
		dec := gob.NewDecoder(s.buffer)
		var a gdb.KeyValue
		err := dec.Decode(&a)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			break
		}
		s.next = i
		out = append(out, a)
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

func ScanDat(data string) *DatReader {
	return &DatReader{
		data: data,
		max:  int(math.Pow(base, exponent)) / (runtime.NumCPU() / int(base)),
	}
}
