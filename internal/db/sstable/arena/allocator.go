package arena

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"sync"
	"time"

	gfile "github.com/blong14/gache/internal/platform/io/file"
)

// Arena https://gist.github.com/quillaja/222c9af7ade058b60ed08e13bf0b6387
type Arena interface {
	Free()
	Bytes() []byte
	Write([]byte) (int, error)
	Append([]byte, []byte) (int, int, error)
	Read([]byte) (int, error)
	ReadAt([]byte, int64, int64) (int, error)
	Cap() uint64
	Len() uint64
}

type goarena struct {
	data    gfile.Map
	buffer  chan KeyValue
	end     uint64
	invalid bool
}

func flusher(a *goarena) {
	ticker := time.NewTicker(100 * time.Millisecond)
	buf := make([]byte, 0)
	buffer := bytes.NewBuffer(buf)
	for {
		select {
		case dat := <-a.buffer:
			buffer.Write(dat.Key)
			buffer.Write([]byte("::"))
			buffer.Write(dat.Value)
			buffer.Write([]byte(";"))
		case <-ticker.C:
			if buffer.Len() > 0 {
				_, _ = a.Write(buffer.Bytes())
				buffer.Reset()
			}
		}
	}
}

func NewGoArena(f *os.File, size uint64) Arena {
	m, err := gfile.NewMap(f, gfile.Prot(gfile.Read), gfile.Prot(gfile.Write), gfile.Flag(gfile.Shared))
	if err != nil {
		panic(err)
	}
	a := &goarena{
		data:    m,
		buffer:  make(chan KeyValue, 4096),
		end:     0,
		invalid: false,
	}
	go flusher(a)

	return a
}

func (a *goarena) Free() {
	err := a.data.Close()
	if err != nil {
		panic(err)
	}
	a.buffer = nil
	a.invalid = true
	a.end = 0
	runtime.GC()
}

func (a *goarena) Write(p []byte) (int, error) {
	return a.data.Write(p)
}

type KeyValue struct {
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value"`
}

var pool = sync.Pool{New: func() interface{} {
	return KeyValue{}
}}

func (a *goarena) Append(k, v []byte) (int, int, error) {
	var out []byte
	buf := bytes.NewBuffer(out)
	buf.Write(k)
	buf.Write([]byte("::"))
	buf.Write(v)
	buf.Write([]byte(";"))
	return a.data.Append(buf.Bytes())
}

func (a *goarena) Read(p []byte) (int, error) {
	return a.data.Read(p)
}

func (a *goarena) ReadAt(p []byte, start, len_ int64) (int, error) {
	kv := make([]byte, len_)
	_, err := a.data.Peek(kv, start, len_)
	if err != nil {
		return -1, err
	}
	values := bytes.Split(kv, []byte("::"))
	if len(values) != 2 {
		return -1, errors.New("malformatted kv pair")
	}
	v := bytes.TrimSuffix(values[1], []byte(";"))
	n := copy(p, v)
	return n, nil
}

func (a *goarena) Bytes() []byte {
	return a.data.Bytes()
}

func (a *goarena) Cap() uint64 { return uint64(a.data.Len()) }
func (a *goarena) Len() uint64 { return uint64(a.data.Pos()) }
