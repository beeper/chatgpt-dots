#!/bin/sh
set -eu

cd "$(dirname "$0")"
export CGO_ENABLED=1
case "$(go env GOOS)" in
    linux|darwin) ;;
    *) printf '%s\n' 'Only Linux and macOS builds are supported (Unix credential ownership checks).' >&2; exit 1 ;;
esac
commit=${CI_COMMIT_SHA:-$(git rev-parse HEAD 2>/dev/null || printf unknown)}
tag=${CI_COMMIT_TAG:-$(git describe --exact-match --tags HEAD 2>/dev/null || printf unknown)}
build_time=${CI_BUILD_TIME:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}
go build -mod=readonly -trimpath -tags goolm \
    -ldflags "-X main.tag=$tag -X main.commit=$commit -X main.buildTime=$build_time" \
    -o chatgpt-dots ./cmd/chatgpt-dots
