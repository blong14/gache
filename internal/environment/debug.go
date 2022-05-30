package environment

import "os"

func Debug() bool {
	return os.Getenv("DEBUG") == "true"
}

func TraceEnabled() bool {
	return os.Getenv("TRACE") == "true"
}
