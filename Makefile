init:
	~/sdk/go1.18/bin/go mod tidy
	~/sdk/go1.18/bin/go mod vendor

bench: clean
	~/sdk/go1.18/bin/go test -cpu=1,4,8 -bench=BenchmarkSkiplist -run=XXX ./...

clean:
	~/sdk/go1.18/bin/go clean --cache --testcache ./...

lint:
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v1.50 golangci-lint run

run: lint
	~/sdk/go1.18/bin/go run github.com/blong14/gache

test:
	~/sdk/go1.18/bin/go test -race -cpu=8 -parallel=8 ./...

build:
	~/sdk/go1.18/bin/go build -o $(PWD)/bin/ github.com/blong14/gache/cmd/...
