package sstable

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"sync"

	garena "github.com/blong14/gache/internal/arena"
	gfile "github.com/blong14/gache/internal/io/file"
	gmap "github.com/blong14/gache/internal/map/tablemap"
)

type SSTable struct {
	mtx   sync.Mutex
	buf   *bufio.Writer
	xindx *gmap.TableMap[[]byte, *indexValue]
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
		xindx: gmap.New[[]byte, *indexValue](bytes.Compare),
		data:  mmap,
		buf:   bufio.NewWriter(f),
	}
}

type indexValue struct {
	length int64
	offset int64
}

func (ss *SSTable) Get(k []byte) ([]byte, bool) {
	raw, ok := ss.xindx.Get(k)
	if !ok {
		return nil, false
	}
	kv := byteArena.Allocate(int(raw.length))
	_, err := ss.data.Peek(kv, raw.offset, raw.length)
	if err != nil {
		return nil, false
	}
	line, err := gfile.DecodeLine(string(kv))
	if err != nil {
		return nil, false
	}
	klen := line[1]
	value := line[klen:]
	return value, true
}

var byteArena = make(garena.ByteArena, 0)

func (ss *SSTable) Set(k, v []byte) error {
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
	offset := ss.ptr
	_len, _ := ss.buf.Write(row)
	_ = ss.buf.Flush()
	ss.ptr += _len
	ss.mtx.Unlock()
	ss.xindx.Set(k, &indexValue{offset: int64(offset), length: int64(_len)})
	return nil
}

func (ss *SSTable) Free() {
	if err := ss.data.Close(); err != nil {
		log.Println(err)
	}
}
