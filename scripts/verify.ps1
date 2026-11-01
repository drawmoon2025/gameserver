# Windows: generate code, build and test. Needs Go 1.25+ and protoc on PATH.
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

function Step($name, [scriptblock]$body) {
    Write-Host "==> $name"
    & $body
    if ($LASTEXITCODE -ne 0) { throw "failed: $name (exit $LASTEXITCODE)" }
}

$goBin = Join-Path (go env GOPATH) 'bin'
$env:PATH = "$goBin;$env:PATH"

Step 'install protoc plugins' {
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
}
Step 'generate protobuf' {
    New-Item -ItemType Directory -Force gen | Out-Null
    protoc --proto_path=proto --go_out=gen --go_opt=paths=source_relative `
        --go-grpc_out=gen --go-grpc_opt=paths=source_relative `
        proto/game/v1/envelope.proto proto/internal/v1/backend.proto
}
Step 'go mod tidy' { go mod tidy }
Step 'go build' { go build ./... }
Step 'go vet' { go vet ./... }
Step 'go test' { go test -count=1 -timeout 120s ./... }
Write-Host 'ALL PASSED'
