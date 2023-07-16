package env

import "os"

func Debug() bool {
	return os.Getenv("DEBUG") == "true"
}
