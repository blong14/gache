
bench: clean
	# ~/sdk/go1.18/bin/go test sync -cpu=1 -bench=BenchmarkLoad -benchmem -run=XXX
	~/sdk/go1.18/bin/go test -v -cpu=1 -bench=Sorted -run=XXX ./...

clean:
	~/sdk/go1.18/bin/go clean --cache --testcache ./...

lint:
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v1.45.2 golangci-lint run -v

run: lint
	~/sdk/go1.18/bin/go run github.com/blong14/gache

test: clean
	~/sdk/go1.18/bin/go test -race -v ./...
