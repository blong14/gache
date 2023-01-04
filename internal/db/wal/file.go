package wal

import (
	"bufio"
	"bytes"
	"os"
	"sync"

	gfile "github.com/blong14/gache/internal/io/file"
)

type WAL struct {
	mtx sync.Mutex
	buf *bufio.Writer
}

func New(f *os.File) *WAL {
	return &WAL{
		buf: bufio.NewWriter(f),
	}
}

var writePool = sync.Pool{New: func() any { return bytes.NewBuffer(nil) }}

func (ss *WAL) Set(k, v []byte) error {
	buf := writePool.Get().(*bytes.Buffer)
	defer writePool.Put(buf)
	buf.Reset()
	buf.Write(k)
	buf.Write([]byte("::"))
	buf.Write(v)
	buf.Write([]byte("\n"))
	encoded := make([]byte, buf.Len())
	copy(encoded, buf.Bytes())
	row, err := gfile.EncodeBlock(encoded)
	if err != nil {
		return err
	}
	ss.mtx.Lock()
	_, _ = ss.buf.Write(row)
	ss.mtx.Unlock()
	_ = ss.buf.Flush()
	return nil
}
