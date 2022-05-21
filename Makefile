init:
	~/sdk/go1.18/bin/go mod tidy
	~/sdk/go1.18/bin/go mod vendor

bench: clean
	~/sdk/go1.18/bin/go test sync -cpu=1 -bench=BenchmarkLoad -benchmem -run=XXX
	~/sdk/go1.18/bin/go test -v -cpu=1 -bench=Sorted -run=XXX ./...

docs:
	~/sdk/go1.18/bin/go doc -all

clean:
	~/sdk/go1.18/bin/go clean --cache --testcache ./...

lint:
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v1.45.2 golangci-lint run

run: lint
	~/sdk/go1.18/bin/go run github.com/blong14/gache

test: lint
	~/sdk/go1.18/bin/go test ./...

build: init lint
	~/sdk/go1.18/bin/go build -o $(PWD)/bin/gctl github.com/blong14/gache/cmd/gctl
	~/sdk/go1.18/bin/go build -o $(PWD)/bin/gache github.com/blong14/gache
