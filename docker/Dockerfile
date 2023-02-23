FROM golang:1.20-bullseye AS go-build

RUN apt-get update

WORKDIR /go/src

COPY go.mod /go/src
COPY go.sum /go/src
RUN go mod download

COPY . /go/src
RUN go build -o /go/bin/ github.com/blong14/gache/cmd/...

FROM debian:bullseye-slim

RUN apt-get update

COPY --from=go-build /go/bin/gache /go/bin/gache
COPY --from=go-build /go/bin/gctl /go/bin/gctl

CMD ["/go/bin/gache"]
