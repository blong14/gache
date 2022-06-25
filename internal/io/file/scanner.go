package file

import (
	"fmt"
	"runtime"

	"github.com/blong14/gache/internal/actors"
	gerrors "github.com/blong14/gache/internal/errors"
)

type Scanner struct {
	start int
	max   int
	buf   []actors.KeyValue
	token []actors.KeyValue
	errs  *gerrors.Error
}

func NewScanner() *Scanner {
	return &Scanner{
		max: 0,
	}
}

func (s *Scanner) Init(kvs []actors.KeyValue) {
	s.start = 0
	s.buf = kvs
	s.max = len(kvs) / runtime.NumCPU()
	s.token = make([]actors.KeyValue, s.max)
}

func (s *Scanner) Rows() []actors.KeyValue {
	return s.token
}

func (s *Scanner) Scan() bool {
	if s.start > len(s.buf) {
		return false
	}
	var idx int
	out := make([]actors.KeyValue, 0)
	for i, kv := range s.buf[s.start:len(s.buf)] {
		idx = s.start + i + 1
		out = append(out, kv)
		if len(out) == s.max {
			s.start = idx
			copy(s.token, out)
			return true
		}
	}
	if len(out) > 0 {
		s.start = idx
		copy(s.token, out)
		return true
	}
	return false
}

func (s *Scanner) String() string {
	return fmt.Sprintf("actors.KeyValue %d", len(s.buf))
}
