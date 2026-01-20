.PHONY: build

GO_CMD := go
BIN := bin

build:
	@mkdir -p bin
	$(GO_CMD) build -o $(BIN)/gateway ./cmd/gateway

run:
	./bin/gateway

test:
	$(GO_CMD) test -v ./...