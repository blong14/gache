GO := ~/sdk/go1.21/bin/go
GOVERSION := go1.21.3
GOLINT := v1.50

bench: $(wildcard ./**/*.go)
	$(GO) test -cpu=1,4,8 -bench=BenchmarkSkiplist -run=XXX ./...

build: $(wildcard ./**/*.go)
	$(GO) build -o $(PWD)/bin/ github.com/blong14/gache/cmd/...

.PHONY: clean
clean:
	$(GO) clean ./...
	rm $(PWD)/bin/* || true
	rm $(PWD)/.deps/* || true

.PHONY: init
init: go.mod go.sum
	$(GO) mod tidy
	$(GO) mod vendor

.PHONY: lint
lint:
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:${GOLINT} golangci-lint run

test:
	$(GO) test -race -cpu=8 -parallel=8 ./...

dl-golang:
	@wget -P .deps https://go.dev/dl/${GOVERSION}.linux-amd64.tar.gz
	@tar -xf .deps/${GOVERSION}.linux-amd64.tar.gz
