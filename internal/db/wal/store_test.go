package wal

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStore_AppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Error(err)
		}
	}()
	s, err := newStore(f)
	if err != nil {
		t.Error(err)
	}
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)
	s, err = newStore(f)
	testRead(t, s)
	if err = s.Close(); err != nil {
		t.Error(err)
	}
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		if err != nil {
			t.Error(err)
		}
		if pos+n != width*i {
			t.Error("missing data")
		}
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(read, write) {
			t.Error("bytes not equal")
		}
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		if err != nil {
			t.Error(err)
		}
		if n != lenWidth {
			t.Error("wrong lenWidth")
		}
		off += int64(n)
		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(b, write) {
			t.Error("wrong bytes")
		}
		if int(size) != n {
			t.Error("wrong size")
		}
		off += int64(n)
	}
}
