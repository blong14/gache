package logging

import (
	"log"
	"os"

	genv "github.com/blong14/gache/internal/environment"
)

func init() {
	if err := os.Setenv("DEBUG", "true"); err != nil {
		log.Fatal(err)
	}
	if err := os.Setenv("TRACE", "false"); err != nil {
		log.Fatal(err)
	}
}

// ShouldLog returns true if in DEBUG mode
func ShouldLog() bool {
	return genv.Debug()
}

// Track logs information IFF the DEBUG env variable is "true"
// error handling should use std log package
func Track(format string, v ...any) {
	if ShouldLog() {
		log.Printf(format, v...)
	}
}
