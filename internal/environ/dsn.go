package environ

import (
	"os"
)

func DSN() string {
	dsn, ok := os.LookupEnv("dsn")
	if !ok {
		// dsn = gache.MEMORY
		dsn = "scrutiny.default"
	}
	return dsn
}
