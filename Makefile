include $(wildcard internal/c/*/build.mk)

GO := ~/sdk/go1.20/bin/go
TAGS := jemalloc

bench: clean build
	$(GO) test -tags=${TAGS} -cpu=1,4,8 -bench=BenchmarkSkiplist -run=XXX ./...

bind:
	$(GO) build -tags=${TAGS} -o $(PWD)/bin/gache.so -buildmode=c-shared github.com/blong14/gache/cmd/bind/...

build: $(wildcard ./**/*.go) build-jemalloc
	$(GO) build -tags=${TAGS} -o $(PWD)/bin/ github.com/blong14/gache/cmd/...

.PHONY: clean
clean: clean-jemalloc
	$(GO) clean --cache --testcache ./...
	rm $(PWD)/bin/* || true
	rm build-jemalloc

.PHONY: init
init: go.mod go.sum
	$(GO) mod tidy
	$(GO) mod vendor

.PHONY: lint
lint:
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v1.50 golangci-lint run

.PHONY: run
run: lint
	$(GO) run github.com/blong14/gache

test:
	$(GO) test -tags=${TAGS} -race -cpu=8 -parallel=8 ./...

.PHONY: dl-golang
dl-golang:
	@wget -P .deps https://go.dev/dl/go1.20.1.linux-amd64.tar.gz
	@tar -xf .deps/go1.20.1.linux-amd64.tar.gz
