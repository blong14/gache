package file

import (
	"github.com/blong14/gache/internal/actors"
	gerrors "github.com/blong14/gache/internal/errors"
)

const maxCount = 6000

type Scanner struct {
	start int
	buf   []actors.KeyValue
	token []actors.KeyValue
	errs  *gerrors.Error
}

func NewScanner() *Scanner {
	return &Scanner{
		token: make([]actors.KeyValue, maxCount),
	}
}

func (s *Scanner) Init(kvs []actors.KeyValue) {
	s.buf = kvs
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
		if len(out) == maxCount {
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
