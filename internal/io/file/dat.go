package file

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
)

var pageSize int

func init() {
	pageSize = os.Getpagesize()
}

func NewDatFile(dir, fileName string) (*os.File, error) {
	file := fmt.Sprintf("%s.dat", fileName)
	p := path.Join(dir, file)
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
		out := []byte{}
		buf := bytes.NewBuffer(out)
		buf.Write([]byte(fmt.Sprintf("begin 0755 %s\n", file)))
		buf.Write(make([]byte, pageSize))
		buf.Write([]byte("\nend\n"))
		_, err = f.Write(buf.Bytes())
		if err != nil {
			return nil, err
		}
	}
	return f, nil
}

const (
	DataStartIndex = 23 // file: begin 0755 default.dat\n<data>\nend\n
	DataEndIndex   = -3
)

var (
	encoding = base64.StdEncoding.WithPadding(base64.NoPadding)
)

type Decoded struct {
	Data     []byte
	Filename string
	Mode     string
}

func Decode(data []byte) (*Decoded, error) {
	dec := &Decoded{}
	if len(data) < 2 {
		return dec, errors.New("invalid decode input")
	}
	rows := strings.Split(string(data), "\n")
	if strings.Split(rows[0], " ")[0] != "begin" {
		return dec, errors.New("invalid format")
	}

	if strings.Split(rows[0], " ")[1] == " " || strings.Split(rows[0], " ")[1] == "" {
		return dec, errors.New("invalid file permissions")
	}
	dec.Mode = strings.Split(rows[0], " ")[1]

	if strings.Split(rows[0], " ")[2] == " " || strings.Split(rows[0], " ")[2] == "" {
		return dec, errors.New("invalid filename")
	}
	dec.Filename = strings.Split(rows[0], " ")[2]

	if rows[len(rows)-2] != "end" {
		return dec, errors.New("invalid format: no 'end' marker found")
	}
	if rows[len(rows)-3] != "`" && rows[len(rows)-3] != " " {
		return dec, errors.New("invalid ending format")
	}

	rows = rows[1 : len(rows)-3]

	var err error
	dec.Data, err = DecodeBlock(rows)
	return dec, err
}

// DecodeBlock decodes a uuencoded text block
func DecodeBlock(rows []string) ([]byte, error) {
	data := []byte{}
	for i, row := range rows {
		res, err := DecodeLine(row)
		if err != nil {
			return data, fmt.Errorf("DecodeBlock at line %d: %s", i+1, err)
		}
		data = append(data, res...)
	}
	return data, nil
}

// DecodeLine decodes a single line of uuencoded text
func DecodeLine(s string) ([]byte, error) {
	if len(s) < 2 {
		return nil, errors.New("invalid line input")
	}

	// fix up non-standard padding `, to make golang's base64 not freak out
	s = strings.ReplaceAll(s, "`", " ")

	// data := []byte(s)
	// l := data[0] - 32 // length
	res, err := encoding.DecodeString(s[1:])
	if err != nil {
		return res, err
	}
	//if len(res) < int(l) {
	//	return nil, errors.New("line decoding failed")
	//}
	return res, nil
}

// Encode encodes data into uuencoded format, with header and footer
func Encode(data []byte, filename, mode string) ([]byte, error) {
	out := []byte{}
	out = append(out, fmt.Sprintf("begin %s %s\n", mode, filename)...)

	enc, err := EncodeBlock(data)
	if err != nil {
		return nil, err
	}
	out = append(out, enc...)

	out = append(out, "`\nend\n"...)
	return out, nil
}

var pool = sync.Pool{New: func() interface{} { return bytes.NewBuffer(nil) }}

// EncodeBlock encodes data in raw uuencoded format
func EncodeBlock(data []byte) ([]byte, error) {
	out := pool.Get().(*bytes.Buffer)
	defer pool.Put(out)
	out.Reset()
	out.WriteByte(byte(len(data)))
	input := make([]byte, base64.StdEncoding.EncodedLen(len(data)))
	encoding.Encode(input, data)
	out.Write(input)
	out.WriteByte('\n')
	o := make([]byte, out.Len())
	copy(o, out.Bytes())
	return o, nil
}
