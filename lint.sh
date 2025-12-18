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

install_linter() {
    echo "Installing golangci-lint v2..."
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
}

# Check if golangci-lint is installed
if ! command -v golangci-lint &> /dev/null; then
    install_linter
fi

# Check version
# The batch file checks for "version 2.". If not found, it upgrades/reinstalls.
if ! golangci-lint version | grep -q "version 2."; then
    echo "Upgrading golangci-lint to v2..."
    install_linter
fi

# Run linter
if golangci-lint run ./...; then
    echo "lint success"
else
    echo -e "\033[0;31mlint error\033[0m"
    exit 1
fi
