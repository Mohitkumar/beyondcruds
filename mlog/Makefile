
GO_CMD := go
BIN := bin
DOCKER_COMPOSE := docker-compose

build:
	@mkdir -p bin
	$(GO_CMD) build -o $(BIN)/mlog .

run:
	$(GO_CMD) run .

test:
	$(GO_CMD) test -v ./...

compile:
	protoc  api/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
