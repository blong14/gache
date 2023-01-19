package wal

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const lenWidth = 8

type store struct {
	*os.File
	mtx  sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(dat []byte) (uint64, uint64, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	pos := s.size
	if err := binary.Write(s.buf, enc, uint64(len(dat))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(dat)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(offset uint64) ([]byte, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(offset)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(offset+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(dat []byte, off int64) (int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(dat, off)
}

func (s *store) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
