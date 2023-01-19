package wal

import (
	"io"
	"os"

	gfile "github.com/blong14/gache/internal/io/file"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}

type index struct {
	file *os.File
	mmap gfile.Map
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	idx.mmap, err = gfile.NewMap(
		f,
		gfile.Prot(gfile.Read),
		gfile.Prot(gfile.Write),
		gfile.Flag(gfile.Shared),
	)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	//if err := i.mmap.Close(); err != nil {
	//	return err
	//}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (uint32, uint64, error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	var out uint32
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos := uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	o := make([]byte, offWidth)
	_, err := i.mmap.Peek(o, int64(pos), int64(pos+offWidth))
	if err != nil {
		return 0, 0, err
	}
	out = enc.Uint32(o)
	p := make([]byte, posWidth)
	_, err = i.mmap.Peek(p, int64(pos+offWidth), int64(pos+entWidth))
	if err != nil {
		return 0, 0, err
	}
	pos = enc.Uint64(p)
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	oout := make([]byte, offWidth)
	enc.PutUint32(oout, off)
	if _, _, err := i.mmap.Append(oout); err != nil {
		return err
	}
	pout := make([]byte, posWidth)
	enc.PutUint64(pout, pos)
	if _, _, err := i.mmap.Append(pout); err != nil {
		return err
	}
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
