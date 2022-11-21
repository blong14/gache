package arena

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"os"
	"runtime"
	"sync"
	"time"
)

// Arena https://gist.github.com/quillaja/222c9af7ade058b60ed08e13bf0b6387
type Arena interface {
	Free()
	Bytes() []byte
	Write([]byte) (int, error)
	XWrite([]byte, []byte) (int, error)
	Read([]byte) (int, error)
	Cap() uint64
	Len() uint64
}

type goarena struct {
	data    Map
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
			var b bytes.Buffer
			enc := gob.NewEncoder(&b)
			if err := enc.Encode(dat); err != nil {
				continue
			}
			b.Write([]byte(";"))
			buffer.Write(b.Bytes())
		case <-ticker.C:
			if buffer.Len() > 0 {
				_, _ = a.Write(buffer.Bytes())
				buffer.Reset()
			}
		}
	}
}

func NewGoArena(f *os.File, size uint64) Arena {
	m, err := NewMap(f, Prot(Read), Prot(Write), Flag(Shared))
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

func (a *goarena) XWrite(k, v []byte) (int, error) {
	kv := pool.Get().(KeyValue)
	defer func() { pool.Put(kv) }()
	kv.Key = k
	kv.Value = v
	a.buffer <- kv
	return 0, nil
}

func (a *goarena) Read(p []byte) (int, error) {
	return a.data.Read(p)
}

func (a *goarena) Bytes() []byte {
	return a.data.Bytes()
}

func (a *goarena) Cap() uint64 { return uint64(a.data.Len()) }
func (a *goarena) Len() uint64 { return uint64(a.data.Pos()) }
