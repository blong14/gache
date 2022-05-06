package csv

import (
	"encoding/csv"
	"io"
	"log"
	"os"
)

func Read(data string) <-chan []string {
	f, err := os.Open(data)
	if err != nil {
		log.Fatal(err)
	}
	csvReader := csv.NewReader(f)
	out := make(chan []string)
	go func() {
		defer close(out)
		defer func() { _ = f.Close() }()
		for {
			row, err := csvReader.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Fatal(err)
			}
			out <- row
		}
	}()
	return out
}
