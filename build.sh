#!/bin/sh
set -eu

# 直接编译独立 Inbox，保留发布脚本使用的二进制名和版本信息。
tag=$(git describe --tags --abbrev=0 2>/dev/null || printf '%s' unknown)
commit=$(git rev-parse --short HEAD 2>/dev/null || printf '%s' unknown)
built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
exec go build -trimpath -buildvcs=false \
    -ldflags "-s -w -X main.Tag=$tag -X main.Commit=$commit -X main.BuildTime=$built_at" \
    -o "${BINARY_NAME:-mautrix-whatsapp}" "$@" ./cmd/mautrix-whatsapp
