package wal

import (
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	gtable "github.com/blong14/gache/internal/map/tablemap"
)

type Log struct {
	mtx           sync.RWMutex
	Dir           string
	Conf          Config
	activeSegment *segment
	segments      *gtable.TableMap[uint64, *segment]
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:  dir,
		Conf: c,
		segments: gtable.New[uint64, *segment](func(a, b uint64) int {
			switch {
			case a < b:
				return -1
			case a == b:
				return 0
			case a > b:
				return 1
			default:
				panic("oops")
			}
		}),
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		ext := path.Ext(file.Name())
		if !strings.HasSuffix(ext, "index") ||
			!strings.HasSuffix(ext, "store") {
			continue
		}
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++
	}
	if l.segments.Size() == 0 {
		if err = l.newSegment(l.Conf.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Conf)
	if err != nil {
		return err
	}
	l.segments.Set(off, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(row []byte) (uint64, error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	off, err := l.activeSegment.Append(row)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Close() error {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.segments.Range(func(k uint64, v *segment) bool {
		return v.Close() != nil
	})
	return nil
}
