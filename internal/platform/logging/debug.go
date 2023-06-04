package logging

import (
	"log"
	"os"
	"time"

	genv "github.com/blong14/gache/internal/platform/env"
)

func init() {
	if err := os.Setenv("DEBUG", "true"); err != nil {
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

func Trace(name string, t time.Time) time.Time {
	if ShouldLog() {
		if !t.IsZero() {
			log.Printf("%s total: %s\n", name, time.Since(t))
		} else {
			log.Printf("tracing %s\n", name)
		}
	}
	return time.Now()
}

func TraceStart(name string) func() time.Time {
	start := Trace(name, time.Time{})
	traceEnd := func() time.Time {
		return Trace(name, start)
	}
	return traceEnd
}
