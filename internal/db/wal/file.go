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

var writePool = sync.Pool{New: func() any { return bytes.NewBuffer(nil) }}

func makeRow(buf *bytes.Buffer, k, v []byte) {
	buf.Write(k)
	buf.Write([]byte("::"))
	buf.Write(v)
	buf.Write([]byte("\n"))
}

var byteArena = make(garena.ByteArena, 0)

func (ss *WAL) Set(k, v []byte) error {
	buf := writePool.Get().(*bytes.Buffer)
	defer writePool.Put(buf)
	buf.Reset()
	makeRow(buf, k, v)
	encoded := byteArena.Allocate(buf.Len())
	copy(encoded, buf.Bytes())
	// row, err := gfile.EncodeBlock(encoded)
	//if err != nil {
	//	return err
	//}
	ss.mtx.Lock()
	_, _ = ss.buf.Write(encoded)
	_ = ss.buf.Flush()
	ss.mtx.Unlock()
	return nil
}
