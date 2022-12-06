package file

import (
	"fmt"
	"os"
	"path"
)

func NewDatFile(dir, fileName string) (*os.File, error) {
	pageSize := os.Getpagesize()
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
		_, err = f.Write(make([]byte, pageSize*pageSize*4))
		if err != nil {
			return nil, err
		}
	}
	return f, nil
}
