FROM golang:1.19-bullseye AS go-build

RUN apt-get update

WORKDIR /go/src

COPY go.mod /go/src
COPY go.sum /go/src
RUN go mod download

COPY . /go/src
RUN go build -o /go/bin/ github.com/blong14/gache

FROM debian:bullseye-slim

RUN apt-get update

COPY --from=go-build /go/bin/gache /go/bin/gache

CMD ["/go/bin/gache"]
