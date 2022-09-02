package environment

import "os"

func Debug() bool {
	return os.Getenv("DEBUG") == "true"
}
