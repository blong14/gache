package encoding

import (
	"bytes"
	"encoding/gob"

	"github.com/blong14/gache/internal/errors"
)

type GCoder struct {
	buf bytes.Buffer
	Err *gerrors.Error
}

func New() *GCoder {
	return &GCoder{
		buf: bytes.Buffer{},
		Err: &gerrors.Error{},
	}
}

func (e *GCoder) HasError() bool {
	return e.Err.ErrorOrNil() != nil
}

func (e *GCoder) Reset() {
	e.buf.Reset()
	e.Err = &gerrors.Error{}
}

func (e *GCoder) Encode(s any) []byte {
	if e.HasError() {
		return nil
	}
	defer e.buf.Reset()
	enc := gob.NewEncoder(&e.buf)
	if err := enc.Encode(s); err != nil {
		e.Err = gerrors.Append(e.Err, err)
		return nil
	}
	return e.buf.Bytes()
}

func (e *GCoder) Decode(data []byte, target any) {
	if e.HasError() {
		return
	}
	defer e.buf.Reset()
	if _, err := e.buf.Read(data); err != nil {
		e.Err = gerrors.Append(e.Err, err)
		return
	}
	dec := gob.NewDecoder(&e.buf)
	if err := dec.Decode(target); err != nil {
		e.Err = gerrors.Append(e.Err, err)
	}
}
