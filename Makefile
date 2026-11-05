SHELL := /bin/bash
.ONESHELL:

export GOWORK=off

THIS_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(abspath $(THIS_DIR)/../..)
QUALITY_TOOLS_BIN := $(REPO_ROOT)/.cache/quality-tools/bin
ENSURE_QUALITY_TOOLS := $(REPO_ROOT)/scripts/ci/ensure-quality-tools.sh
GOCACHE ?= $(REPO_ROOT)/.cache/go-build
GOMODCACHE ?= $(REPO_ROOT)/.cache/go-mod
GOLANGCI_LINT_CACHE ?= $(REPO_ROOT)/.cache/golangci-v1-client
export GOCACHE
export GOMODCACHE
export GOLANGCI_LINT_CACHE

BIN_DIR ?= ./bin
ARTIFACTS_DIR ?= ./dist
VERSION ?= dev
TARGET_OS := $(or $(GOOS),$(shell go env GOOS))
BINARY_NAME := $(if $(filter windows,$(TARGET_OS)),client.msi,client)
DEFAULT_SERVER_URL ?= https://fortunnels.ru

.PHONY: all build build-fast test tidy clean release release-dev format format-check lint security check install-tools quality-tools

all: build

# Callers that already ran the bootstrap (apps/Makefile, check) pass QUALITY_TOOLS_READY=1 to skip a second run.
quality-tools:
	@[ -n "$(QUALITY_TOOLS_READY)" ] || $(ENSURE_QUALITY_TOOLS)

install-tools: quality-tools
	@echo "==> Pinned repository-local quality tools are ready"

tidy:
	@echo "==> go mod tidy (client)"
	go mod tidy

test:
	@echo "==> go test -race -shuffle=on ./..."
	go test -race -shuffle=on ./...

build: check
	@echo "==> go build (client)"
	mkdir -p $(BIN_DIR)
	go build -ldflags "-X main.version=$(VERSION) -X main.defaultServerURL=$(DEFAULT_SERVER_URL)" -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/client


build-fast:
	@echo "==> go build (client, fast)"
	mkdir -p $(BIN_DIR)
	go build -trimpath -ldflags "-X main.version=$(VERSION) -X main.defaultServerURL=$(DEFAULT_SERVER_URL)" -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/client

clean:
	rm -rf $(BIN_DIR) $(ARTIFACTS_DIR)

# Formatters walk only the module's own sources, never local caches or build output.
GO_SOURCES = $(shell find . \( -path ./.cache -o -path ./bin -o -path ./dist -o -path ./graphify-out \) -prune -o -name '*.go' -print)

format: quality-tools
	@echo "==> gofumpt + goimports (rewrite)"
	@GOWORK=off $(QUALITY_TOOLS_BIN)/gofumpt -w $(GO_SOURCES)
	@GOWORK=off $(QUALITY_TOOLS_BIN)/goimports -local github.com/fortunnels -w $(GO_SOURCES)

format-check: quality-tools
	@set -euo pipefail; \
	echo "==> Checking formatting (read-only)"; \
	unformatted=$$(GOWORK=off $(QUALITY_TOOLS_BIN)/gofumpt -l $(GO_SOURCES)); \
	if [ -n "$$unformatted" ]; then \
		echo "ERROR: Unformatted Go files (run 'make format'):"; \
		echo "$$unformatted"; \
		exit 1; \
	fi; \
	unimported=$$(GOWORK=off $(QUALITY_TOOLS_BIN)/goimports -local github.com/fortunnels -l $(GO_SOURCES)); \
	if [ -n "$$unimported" ]; then \
		echo "ERROR: Go files with import issues (run 'make format'):"; \
		echo "$$unimported"; \
		exit 1; \
	fi

# The client is public, so lint also keeps its sources gofumpt/goimports clean for readers.
lint: quality-tools format-check
	@GOWORK=off $(QUALITY_TOOLS_BIN)/golangci-lint run --config .golangci.yml ./...
	@GOWORK=off $(QUALITY_TOOLS_BIN)/golangci-lint run --config .golangci-structure.yml --allow-parallel-runners ./...

security: quality-tools
	@GOWORK=off $(QUALITY_TOOLS_BIN)/govulncheck ./...

# golangci-lint (the lint target) also runs staticcheck, gosimple, stylecheck, misspell and gocyclo.
check: quality-tools
	@set -euo pipefail; \
	echo "==> Running strict code checks (will fail build on any error)"; \
	echo "Running go vet..."; \
	go vet ./...; \
	echo "Running tests..."; \
	go test -v ./...; \
	echo "Running format check and golangci-lint..."; \
	$(MAKE) --no-print-directory lint QUALITY_TOOLS_READY=1; \
	echo "Running security check..."; \
	$(QUALITY_TOOLS_BIN)/govulncheck ./...; \
	echo "All checks passed"

release: tidy
	@set -euo pipefail; \
	echo "==> Building client release $(VERSION)"; \
	mkdir -p "$(ARTIFACTS_DIR)"; \
	LDFLAGS="-s -w -X main.version=$(VERSION)"; \
	if [ -n "$(DEFAULT_SERVER_URL)" ]; then \
		LDFLAGS="$$LDFLAGS -X main.defaultServerURL=$(DEFAULT_SERVER_URL)"; \
	fi; \
	build_tar() { \
		OS="$$1"; ARCH="$$2"; SUFFIX="$$3"; \
		echo "   -> $$OS/$$ARCH (tar.gz)"; \
		CGO_ENABLED=0 GOOS="$$OS" GOARCH="$$ARCH" go build -ldflags "$$LDFLAGS" -o /tmp/client ./cmd/client; \
		mv /tmp/client /tmp/fortunnels; \
		tar -C /tmp -czf "$(ARTIFACTS_DIR)/fortunnels-$$SUFFIX.tar.gz" fortunnels; \
		rm -f /tmp/fortunnels; \
	}; \
	build_zip() { \
		ARCH="$$1"; SUFFIX="$$2"; \
		echo "   -> windows/$$ARCH (zip)"; \
		CGO_ENABLED=0 GOOS=windows GOARCH="$$ARCH" go build -ldflags "$$LDFLAGS" -o /tmp/client.msi ./cmd/client; \
		mv /tmp/client.msi /tmp/fortunnels.msi; \
		zip -j -q "$(ARTIFACTS_DIR)/fortunnels-$$SUFFIX.zip" /tmp/fortunnels.msi; \
		cp /tmp/fortunnels.msi "$(ARTIFACTS_DIR)/fortunnels-$$SUFFIX.msi"; \
		rm -f /tmp/fortunnels.msi; \
	}; \
	build_tar darwin amd64 macos+amd64; \
	build_tar darwin arm64 macos+arm64; \
	build_tar linux amd64 linux+amd64; \
	build_tar linux arm64 linux+arm64; \
	build_zip amd64 windows+amd64; \
	build_zip arm64 windows+arm64; \
	build_zip 386 windows+x86; \
	# MSIX fallback: Creates a copy of Windows amd64 executable as .msix \
	# for basic compatibility. For proper MSIX packaging, use: make msix-package \
	if [ ! -f "$(ARTIFACTS_DIR)/fortunnels.msix" ]; then \
		if [ -f "$(ARTIFACTS_DIR)/fortunnels-windows+amd64.msi" ]; then \
			cp "$(ARTIFACTS_DIR)/fortunnels-windows+amd64.msi" "$(ARTIFACTS_DIR)/fortunnels.msix"; \
			echo "   -> Created fallback MSIX package (copy of Windows amd64 executable)"; \
		else \
			echo "Warning: fortunnels-windows+amd64.msi not found. MSIX fallback was not created."; \
		fi; \
	fi; \
	cd "$(ARTIFACTS_DIR)" && shasum -a 256 fortunnels-* > SHA256SUMS.txt; \
	echo "==> Artifacts saved to $(ARTIFACTS_DIR)"

release-dev:
	$(MAKE) release VERSION=$(VERSION) DEFAULT_SERVER_URL="$(DEFAULT_SERVER_URL)"
