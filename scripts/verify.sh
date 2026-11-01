#!/usr/bin/env bash
# Linux/macOS: generate code, build and test. Needs Go 1.25+ and protoc on PATH.
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$(go env GOPATH)/bin:$PATH"

echo "==> install protoc plugins"
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

echo "==> generate protobuf"
mkdir -p gen
protoc --proto_path=proto --go_out=gen --go_opt=paths=source_relative \
  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
  proto/game/v1/envelope.proto proto/backend/v1/backend.proto

echo "==> go mod tidy"; go mod tidy
echo "==> go build"; go build ./...
echo "==> go vet"; go vet ./...
echo "==> go test"; go test -count=1 -timeout 120s ./...
echo "ALL PASSED"
