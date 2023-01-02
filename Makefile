GO=~/sdk/go1.18/bin/go

bench: clean
	$(GO) test -cpu=1,4,8 -bench=BenchmarkSkiplist -run=XXX ./...

bind:
	$(GO) build -o $(PWD)/bin/gache.so -buildmode=c-shared github.com/blong14/gache/cmd/bind/...

build:
	$(GO) build -o $(PWD)/bin/ github.com/blong14/gache/cmd/...

clean:
	$(GO) clean --cache --testcache ./...
	rm $(PWD)/bin/*

init: go.mod go.sum
	$(GO) mod tidy
	$(GO) mod vendor

lint:
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v1.50 golangci-lint run

run: lint
	$(GO) run github.com/blong14/gache

test:
	$(GO) test -race -cpu=8 -parallel=8 ./...

