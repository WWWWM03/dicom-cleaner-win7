#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$ROOT_DIR"
mkdir -p .gocache

export GOOS=windows
export GOARCH=386
export CGO_ENABLED=0
export GOCACHE="$ROOT_DIR/.gocache"
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOSUMDB="${GOSUMDB:-off}"

go build -mod=vendor -o dicom_cleaner.exe ./cmd/dicomcleaner

echo "Built: $ROOT_DIR/dicom_cleaner.exe"
