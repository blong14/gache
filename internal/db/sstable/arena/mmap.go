package arena

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
)

// Adapted from https://github.com/johnsiilver/golib

const (
	Read  = syscall.PROT_READ
	Write = syscall.PROT_WRITE
	Exec  = syscall.PROT_EXEC

	Shared = syscall.MAP_SHARED
)

type Map interface {
	io.ReadWriteCloser
	io.Seeker
	io.ReaderAt
	Bytes() []byte
	Len() int
	Pos() int
	Lock() error
	Unlock() error
	Append([]byte) (int, int, error)
	Peek([]byte, int64, int64) (int, error)
}

type Option func(m *mmap)

func Prot(p int) Option {
	return func(m *mmap) {
		if p == Write {
			m.write = true
		}
		if m.prot != -1 {
			m.prot = m.prot | p
			return
		}
		m.prot = p
	}
}

func Flag(f int) Option {
	return func(m *mmap) {
		m.flags = f
	}
}

func Length(s int) Option {
	return func(m *mmap) {
		m.len = s
	}
}

func Offset(o int64) Option {
	return func(m *mmap) {
		m.offset = o
	}
}

func NewMap(f *os.File, opts ...Option) (Map, error) {
	return newMap(f, opts...)
}

func newMap(f *os.File, opts ...Option) (*mmap, error) {
	m := &mmap{
		f:     f,
		flags: -1,
		prot:  -1,
		len:   -1,
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.flags == -1 || m.prot == -1 {
		return nil, errors.New("must pass options to set the flag or prot values")
	}

	s, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if s.Size() == 0 {
		return nil, errors.New("cannot mmap 0 length file")
	}
	if m.len == -1 {
		m.len = int(s.Size())
	}

	m.data, err = syscall.Mmap(int(f.Fd()), m.offset, m.len, m.prot, m.flags)
	if err != nil {
		return nil, fmt.Errorf("problem with mmap system call: %w", err)
	}

	return m, nil
}

type mmap struct {
	sync.RWMutex
	flags, prot, len int
	offset           int64
	data             []byte
	ptr              int
	write            bool
	f                *os.File
}

func (m *mmap) Bytes() []byte {
	m.RLock()
	defer m.RUnlock()
	return m.data
}

func (m *mmap) Len() int {
	return m.len
}

func (m *mmap) Read(p []byte) (int, error) {
	m.RLock()
	defer m.RUnlock()
	if m.ptr >= m.len {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.ptr:])
	m.ptr += n
	if n == m.len-m.ptr {
		return n, io.EOF
	}
	return n, nil
}

func (m *mmap) ReadAt(p []byte, off int64) (int, error) {
	m.RLock()
	defer m.RUnlock()
	if int(off) >= m.len {
		return 0, errors.New("offset is larger than the mmap []byte")
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, errors.New("len(p) was greater than mmap[off:]")
	}
	return n, nil
}

func (m *mmap) Peek(p []byte, start, length int64) (int, error) {
	m.RLock()
	defer m.RUnlock()
	if int(start) >= m.len || int(start+length) >= m.len {
		return 0, errors.New("offset is larger than the mmap []byte")
	}
	n := copy(p, m.data[start:start+length])
	if n < len(p) {
		return n, errors.New("len(p) was greater than mmap[off:]")
	}
	return n, nil
}

func (m *mmap) Write(p []byte) (int, error) {
	err := m.Lock()
	if err != nil {
		return 0, fmt.Errorf("cannot lock memory: %w", err)
	}
	defer func() { _ = m.Unlock() }()
	if !m.write {
		return 0, errors.New("cannot write to non-writeable mmap")
	}
	n := copy(m.data[m.ptr:], p)
	m.ptr += n
	return n, nil
}

func (m *mmap) Append(p []byte) (int, int, error) {
	err := m.Lock()
	if err != nil {
		return 0, 0, fmt.Errorf("cannot lock memory: %w", err)
	}
	defer func() { _ = m.Unlock() }()
	if !m.write {
		return 0, 0, errors.New("cannot write to non-writeable mmap")
	}
	offset := m.ptr
	n := copy(m.data[m.ptr:], p)
	m.ptr += n
	return n, offset, nil
}

func (m *mmap) Seek(offset int64, whence int) (int64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("cannot seek to a negative offset")
	}
	err := m.Lock()
	if err != nil {
		return 0, fmt.Errorf("cannot lock memory: %w", err)
	}
	defer func() { _ = m.Unlock() }()

	switch whence {
	case 0:
		if offset < int64(m.len) {
			m.ptr = int(offset)
			return int64(m.ptr), nil
		}
		return 0, errors.New("offset goes beyond the data size")
	case 1:
		if m.ptr+int(offset) < m.len {
			m.ptr += int(offset)
			return int64(m.ptr), nil
		}
		return 0, errors.New("offset goes beyond the data size")
	case 2:
		if m.ptr-int(offset) > -1 {
			m.ptr -= int(offset)
			return int64(m.ptr), nil
		}
		return 0, errors.New("offset would set the offset as a negative number")
	}
	return 0, errors.New("whence arg was not set to a valid value")
}

func (m *mmap) Pos() int {
	m.RLock()
	defer m.RUnlock()
	return m.ptr
}

func (m *mmap) Lock() error {
	m.RLock()
	defer m.RUnlock()
	return syscall.Mlock(m.data)
}

func (m *mmap) Unlock() error {
	m.RLock()
	defer m.RUnlock()
	return syscall.Munlock(m.data)
}

func (m *mmap) Close() error {
	m.RLock()
	defer m.RUnlock()
	defer func() { _ = m.f.Close() }()
	return syscall.Munmap(m.data)
}
