package sstable

import (
	"bytes"
	"log"
	"os"
	"sync"

	gmmap "github.com/blong14/gache/internal/db/sstable/arena"
)

type SSTable struct {
	index *sync.Map
	data  gmmap.Map
}

// New creates a new ready to use SSTable
//
// f := os.Open("file")
// s := New(f)
// err := s.Open()
// if err != nil {
//   panic(err)
// }
// defer s.Free()
// s.SetByHash(k, v)
// v, ok := s.GetByHash(k)
func New(f *os.File) *SSTable {
	mmap, err := gmmap.NewMap(
		f,
		gmmap.Prot(gmmap.Read),
		gmmap.Prot(gmmap.Write),
		gmmap.Flag(gmmap.Shared),
	)
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
	values := bytes.Split(kv, []byte("::"))
	if len(values) != 2 {
		return nil, false
	}
	value := bytes.TrimSuffix(values[1], []byte(";"))
	return value, true
}

func (ss *SSTable) Set(k, v []byte) error {
	buf := bytes.NewBuffer(k)
	buf.Write([]byte("::"))
	buf.Write(v)
	buf.Write([]byte(";"))
	len_, offset, err := ss.data.Append(buf.Bytes())
	if err != nil {
		return err
	}
	ss.index.Store(string(k), &indexValue{offset: int64(offset), length: int64(len_)})
	return nil
}

func (ss *SSTable) Free() {
	if err := ss.data.Close(); err != nil {
		log.Println(err)
	}
	ss.index = nil
}
