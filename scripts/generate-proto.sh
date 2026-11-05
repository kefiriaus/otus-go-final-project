#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$root/api/stats/v1/tools.env"
if ! command -v protoc >/dev/null 2>&1; then
  echo "protoc $PROTOC_VERSION is required; see api/stats/v1/README.md" >&2
  exit 1
fi
actual=$(protoc --version)
if [ "$actual" != "libprotoc $PROTOC_VERSION" ]; then
  echo "expected protoc $PROTOC_VERSION; got $actual" >&2
  exit 1
fi
tools_dir=$(mktemp -d)
trap 'rm -rf "$tools_dir"' EXIT
trap 'exit 1' HUP INT TERM
GOBIN="$tools_dir" go install "google.golang.org/protobuf/cmd/protoc-gen-go@$PROTOC_GEN_GO_VERSION"
GOBIN="$tools_dir" go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$PROTOC_GEN_GO_GRPC_VERSION"
PATH="$tools_dir:$PATH"
export PATH
cd "$root"
protoc -I . \
  --go_out=. --go_opt=module=github.com/OtusGolang/final_project \
  --go-grpc_out=. --go-grpc_opt=module=github.com/OtusGolang/final_project \
  api/stats/v1/stats.proto
