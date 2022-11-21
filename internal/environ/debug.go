package environ

import "os"

func Debug() bool {
	return os.Getenv("DEBUG") == "true"
}
