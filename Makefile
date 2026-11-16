GO ?= go
DOCKER ?= docker
BIN_DIR ?= bin
DOCKER_IMAGE ?= otus-stats-daemon:local
DOCKER_BUILD_ARGS ?=
GORACE ?= atexit_sleep_ms=0

.PHONY: build test test-race vet lint check docker-build

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/stats-daemon ./cmd/stats-daemon
	$(GO) build -o $(BIN_DIR)/stats-client ./cmd/stats-client

test:
	$(GO) test ./...

test-race:
	GORACE="$(GORACE)" $(GO) test -race -count=100 ./...

vet:
	$(GO) vet ./...

lint:
	golangci-lint run ./...

check: build vet test-race lint

docker-build:
	$(DOCKER) build $(DOCKER_BUILD_ARGS) -t $(DOCKER_IMAGE) .
