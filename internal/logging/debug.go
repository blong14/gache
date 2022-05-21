package logging

import (
	"log"
	"os"
)

func init() {
	if err := os.Setenv("DEBUG", "true"); err != nil {
		log.Fatal(err)
	}
}

// ShouldLog returns true if in DEBUG mode
func ShouldLog() bool {
	return os.Getenv("DEBUG") == "true"
}

// Track logs information IFF the DEBUG env variable is "true"
// error handling should use std log package
func Track(format string, v ...any) {
	if ShouldLog() {
		log.Printf(format, v...)
	}
}
