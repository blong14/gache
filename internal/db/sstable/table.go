package sstable

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"sync"

	garena "github.com/blong14/gache/internal/db/arena"
	gfile "github.com/blong14/gache/internal/io/file"
)

type SSTable struct {
	mtx   sync.Mutex
	buf   *bufio.Writer
	index *sync.Map
	data  gfile.Map
	ptr   int
}

func New(f *os.File) *SSTable {
	s, err := f.Stat()
	if err != nil {
		panic(err)
	}
	len_ := s.Size() + gfile.DataEndIndex
	mmap, err := gfile.NewMap(
		f,
		gfile.Prot(gfile.Read),
		gfile.Prot(gfile.Write),
		gfile.Flag(gfile.Shared),
		gfile.Length(int(len_)),
	)
	if err != nil {
		panic(err)
	}
	_, err = mmap.Seek(gfile.DataStartIndex, 0)
	if err != nil {
		panic(err)
	}
	_, err = f.Seek(gfile.DataStartIndex, 0)
	if err != nil {
		panic(err)
	}
	return &SSTable{
		index: &sync.Map{},
		data:  mmap,
		buf:   bufio.NewWriter(f),
	}
}

type indexValue struct {
	length int64
	offset int64
}

func (ss *SSTable) Get(k []byte) ([]byte, bool) {
	raw, ok := ss.index.Load(string(k))
	if !ok {
		return nil, false
	}
	v, ok := raw.(*indexValue)
	if !ok {
		return nil, false
	}
	kv := make([]byte, v.length)
	_, err := ss.data.Peek(kv, v.offset, v.length)
	if err != nil {
		return nil, false
	}
	line, err := gfile.DecodeLine(string(kv))
	if err != nil {
		return nil, false
	}
	values := bytes.Split(line[1:len(line)-1], []byte("::"))
	if len(values) != 2 {
		return nil, false
	}
	return values[1], true
}

var writePool = sync.Pool{New: func() any { return bytes.NewBuffer(nil) }}

var byteArena = make(garena.ByteArena, 0)

func (ss *SSTable) Set(k, v []byte) error {
	buf := writePool.Get().(*bytes.Buffer)
	defer writePool.Put(buf)
	buf.Reset()
	buf.Write(k)
	buf.Write([]byte("::"))
	buf.Write(v)
	buf.Write([]byte("\n"))
	encoded := byteArena.Allocate(buf.Len())
	copy(encoded, buf.Bytes())
	//row, err := gfile.EncodeBlock(encoded)
	//if err != nil {
	//	return err
	//}
	ss.mtx.Lock()
	offset := ss.ptr
	_len, _ := ss.buf.Write(encoded)
	_ = ss.buf.Flush()
	ss.ptr += _len
	ss.mtx.Unlock()
	ss.index.Store(string(k), &indexValue{offset: int64(offset), length: int64(_len)})
	return nil
}

func (ss *SSTable) Free() {
	if err := ss.data.Close(); err != nil {
		log.Println(err)
	}
}
