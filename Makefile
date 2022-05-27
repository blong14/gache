init:
	~/sdk/go1.18/bin/go mod tidy
	~/sdk/go1.18/bin/go mod vendor

# ~/sdk/go1.18/bin/go test sync -cpu=1 -bench=BenchmarkLoad -benchmem -run=XXX
bench: clean
	~/sdk/go1.18/bin/go test -race -cpu=2 -bench=BenchmarkConcurrent -run=XXX ./internal/cache/sorted/treemap/...
	~/sdk/go1.18/bin/go test -bench=BenchmarkConcurrent -run=XXX ./...

docs:
	~/sdk/go1.18/bin/go doc -all

clean:
	~/sdk/go1.18/bin/go clean --cache --testcache ./...

lint:
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v1.45.2 golangci-lint run

run: lint
	~/sdk/go1.18/bin/go run github.com/blong14/gache

test:
	~/sdk/go1.18/bin/go test -race -cpu=8 -parallel=8 ./...

build: init
	~/sdk/go1.18/bin/go build -o $(PWD)/bin/gctl github.com/blong14/gache/cmd/gctl
	~/sdk/go1.18/bin/go build -o $(PWD)/bin/gache github.com/blong14/gache
