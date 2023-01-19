package wal

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

type entry struct {
	Off uint32
	Pos uint64
}

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			t.Error(err)
		}
	}()
	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	if err != nil {
		t.Error(err)
	}
	if f.Name() != idx.Name() {
		t.Error("wrong name")
	}
	entries := []entry{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}
	testIndexWrite(t, idx, entries)
	testIndexRead(t, idx, entries)
}

func testIndexWrite(t *testing.T, idx *index, entries []entry) {
	for _, want := range entries {
		err := idx.Write(want.Off, want.Pos)
		if err != nil {
			t.Error(err)
		}
		_, pos, err := idx.Read(int64(want.Off))
		if err != nil {
			t.Error(err)
		}
		if want.Pos != pos {
			t.Error("wrong pos")
		}
	}
}

func testIndexRead(t *testing.T, idx *index, entries []entry) {
	_, _, err := idx.Read(int64(len(entries)))
	if !errors.Is(err, io.EOF) {
		t.Error(err)
	}
	_ = idx.Close()
	f, _ := os.OpenFile(idx.Name(), os.O_RDWR, 0600)
	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err = newIndex(f, c)
	if err != nil {
		t.Error(err)
	}
	off, pos, err := idx.Read(-1)
	if err != nil {
		t.Error(err)
	}
	if uint32(1) != off {
		t.Error("wrong offset")
	}
	if entries[1].Pos != pos {
		t.Error("wrong pos")
	}
}
