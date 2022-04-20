
bench:
	~/sdk/go1.18/bin/go test -cpu=1 -bench=TableMap -run=XXX ./...

run:
	~/sdk/go1.18/bin/go run main.go

test:
	~/sdk/go1.18/bin/go test -race -v ./...
