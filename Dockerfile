# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v3 \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -pgo=auto \
      -o /out/inesdb ./cmd/cli

FROM alpine:latest

RUN addgroup -S inesdb && adduser -S inesdb -G inesdb

WORKDIR /data
RUN chown inesdb:inesdb /data

COPY --from=builder /out/inesdb /usr/local/bin/inesdb

USER inesdb
EXPOSE 5432

ENTRYPOINT ["inesdb", "-db=/data/test.db", "-lsm-data-dir=/data/lsm_data", "-wal=/data/test.wal"]