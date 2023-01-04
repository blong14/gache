package sstable

import (
	"bytes"
	"log"
	"os"
	"sync"

	gmmap "github.com/blong14/gache/internal/db/sstable/arena"
	gfile "github.com/blong14/gache/internal/io/file"
)

type SSTable struct {
	index *sync.Map
	data  gmmap.Map
}

func New(f *os.File) *SSTable {
	s, err := f.Stat()
	if err != nil {
		panic(err)
	}
	len_ := s.Size() + gfile.DataEndIndex
	mmap, err := gmmap.NewMap(
		f,
		gmmap.Prot(gmmap.Read),
		gmmap.Prot(gmmap.Write),
		gmmap.Flag(gmmap.Shared),
		gmmap.Length(int(len_)),
	)
	if err != nil {
		panic(err)
	}
	_, err = mmap.Seek(gfile.DataStartIndex, 0)
	if err != nil {
		panic(err)
	}
	return &SSTable{
		index: &sync.Map{},
		data:  mmap,
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

func (ss *SSTable) Set(k, v []byte) error {
	buf := writePool.Get().(*bytes.Buffer)
	defer writePool.Put(buf)
	buf.Reset()
	buf.Write(k)
	buf.Write([]byte("::"))
	buf.Write(v)
	buf.Write([]byte("\n"))
	encoded := make([]byte, buf.Len())
	copy(encoded, buf.Bytes())

	//row, err := gfile.EncodeBlock(encoded)
	//if err != nil {
	//	return err
	//}
	len_, offset, err := ss.data.Append(encoded)
	if err != nil {
		return err
	}
	ss.index.Store(string(k), &indexValue{offset: int64(offset), length: int64(len_)})
	return nil
}

func (ss *SSTable) XSet(buf *bytes.Buffer) error {
	_, _, err := ss.data.Append(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (ss *SSTable) Free() {
	if err := ss.data.Close(); err != nil {
		log.Println(err)
	}
	ss.index = nil
}
