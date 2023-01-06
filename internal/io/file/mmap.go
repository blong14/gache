package file

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
	MLock() error
	MUnlock() error
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

func (m *mmap) Read(p []byte) (int, error) {
	if m.Pos() >= m.Len() {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.Pos():])
	m.Lock()
	m.ptr += n
	m.Unlock()
	if n == m.Len()-m.Pos() {
		return n, io.EOF
	}
	return n, nil
}

func (m *mmap) ReadAt(p []byte, off int64) (int, error) {
	if int(off) >= m.Len() {
		return 0, errors.New("offset is larger than the mmap []byte")
	}
	m.RLock()
	n := copy(p, m.data[off:])
	m.RUnlock()
	if n < len(p) {
		return n, errors.New("len(p) was greater than mmap[off:]")
	}
	return n, nil
}

func (m *mmap) Peek(p []byte, start, length int64) (int, error) {
	if int(start) >= m.Len() || int(start+length) >= m.Len() {
		return 0, errors.New("offset is larger than the mmap []byte")
	}
	m.RLock()
	n := copy(p, m.data[start:start+length])
	m.RUnlock()
	if n < len(p) {
		return n, errors.New("len(p) was greater than mmap[off:]")
	}
	return n, nil
}

func (m *mmap) Write(p []byte) (int, error) {
	err := m.MLock()
	if err != nil {
		return 0, fmt.Errorf("cannot lock memory: %w", err)
	}
	defer func() { _ = m.MUnlock() }()
	if !m.write {
		return 0, errors.New("cannot write to non-writeable mmap")
	}
	m.Lock()
	n := copy(m.data[m.ptr:], p)
	m.ptr += n
	m.Unlock()
	return n, nil
}

func (m *mmap) Append(p []byte) (int, int, error) {
	err := m.MLock()
	if err != nil {
		return 0, 0, fmt.Errorf("cannot lock memory: %w", err)
	}
	defer func() { _ = m.MUnlock() }()
	if !m.write {
		return 0, 0, errors.New("cannot write to non-writeable mmap")
	}
	offset := m.Pos()
	m.Lock()
	n := copy(m.data[m.ptr:], p)
	m.ptr += n
	m.Unlock()
	return n, offset, nil
}

func (m *mmap) Seek(offset int64, whence int) (int64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("cannot seek to a negative offset")
	}
	err := m.MLock()
	if err != nil {
		return 0, fmt.Errorf("cannot lock memory: %w", err)
	}
	defer func() { _ = m.MUnlock() }()

	switch whence {
	case 0:
		if offset < int64(m.Len()) {
			m.Lock()
			m.ptr = int(offset)
			m.Unlock()
			return int64(m.Pos()), nil
		}
		return 0, errors.New("offset goes beyond the data size")
	case 1:
		if m.Pos()+int(offset) < m.Len() {
			m.Lock()
			m.ptr += int(offset)
			m.Unlock()
			return int64(m.Pos()), nil
		}
		return 0, errors.New("offset goes beyond the data size")
	case 2:
		if m.Pos()-int(offset) > -1 {
			m.Lock()
			m.ptr -= int(offset)
			m.Unlock()
			return int64(m.Pos()), nil
		}
		return 0, errors.New("offset would set the offset as a negative number")
	default:
		return 0, errors.New("whence arg was not set to a valid value")
	}
}

func (m *mmap) Bytes() []byte {
	m.RLock()
	defer m.RUnlock()
	return m.data
}

func (m *mmap) Len() int {
	m.RLock()
	defer m.RUnlock()
	return m.len
}

func (m *mmap) Pos() int {
	m.RLock()
	defer m.RUnlock()
	return m.ptr
}

func (m *mmap) MLock() error {
	m.RLock()
	defer m.RUnlock()
	return syscall.Mlock(m.data)
}

func (m *mmap) MUnlock() error {
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
