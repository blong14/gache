package wal

import (
	"bufio"
	"bytes"
	"os"
	"sync"

	garena "github.com/blong14/gache/internal/db/arena"
	gfile "github.com/blong14/gache/internal/io/file"
)

type WAL struct {
	mtx sync.Mutex
	buf *bufio.Writer
}

func New(f *os.File) *WAL {
	s, err := f.Stat()
	if err != nil {
		panic(err)
	}
	size := s.Size()
	if size == 0 {
		buf := bytes.NewBuffer(nil)
		buf.Write(gfile.DatFileHeader(f.Name()))
		_, err = f.Write(buf.Bytes())
		if err != nil {
			panic(err)
		}
	}
	return &WAL{
		buf: bufio.NewWriter(f),
	}
}

var byteArena = make(garena.ByteArena, 0)

func (ss *WAL) Set(k, v []byte) error {
	klen := len(k)
	vlen := len(v)
	encoded := byteArena.Allocate(klen + vlen + 1)
	encoded[0] = byte(klen)
	copy(encoded[1:klen+1], k)
	copy(encoded[klen+1:], v)
	row, err := gfile.EncodeBlock(encoded)
	if err != nil {
		return err
	}
	ss.mtx.Lock()
	_, _ = ss.buf.Write(row)
	_ = ss.buf.Flush()
	ss.mtx.Unlock()
	return nil
}
