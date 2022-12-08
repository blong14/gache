package file

import (
	"fmt"
	"os"
	"path"
)

var pageSize int

func init() {
	pageSize = os.Getpagesize()
}

func NewDatFile(dir, fileName string) (*os.File, error) {
	p := path.Join(dir, fmt.Sprintf("%s.dat", fileName))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	s, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := s.Size()
	if size == 0 {
		// memory ballast
		_, err = f.Write(make([]byte, pageSize*pageSize))
		if err != nil {
			return nil, err
		}
	}
	return f, nil
}
