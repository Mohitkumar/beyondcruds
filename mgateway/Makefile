.PHONY: build

GO_CMD := go
BIN := bin

build:
	@mkdir -p bin
	$(GO_CMD) build -o $(BIN)/gateway .

run:
	$(GO_CMD) run .

test:
	$(GO_CMD) test -v ./...