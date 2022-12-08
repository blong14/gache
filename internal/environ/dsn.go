package environ

import (
	"os"
)

func DSN() string {
	dsn, ok := os.LookupEnv("dsn")
	if !ok {
		dsn = ":memory:"
	}
	return dsn
}

func DataDir() string {
	dir, ok := os.LookupEnv("datadir")
	if !ok {
		dir = "data"
	}
	return dir
}
