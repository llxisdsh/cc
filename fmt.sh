#!/bin/bash
set -e

if ! command -v go &> /dev/null; then
    echo "go not found in PATH"
    exit 1
fi

GOBIN=$(go env GOBIN)
if [ -z "$GOBIN" ]; then
    # GOPATH can be a list. Use the first one.
    GOPATH=$(go env GOPATH | cut -d: -f1)
    if [ -z "$GOPATH" ]; then
        GOPATH="$HOME/go"
    fi
    GOBIN="$GOPATH/bin"
fi
export PATH="$GOBIN:$PATH"

if ! command -v gofumpt &> /dev/null; then
    echo "Installing gofumpt..."
    go install mvdan.cc/gofumpt@latest
fi

gofumpt -l -w .
gofmt -s -w .
