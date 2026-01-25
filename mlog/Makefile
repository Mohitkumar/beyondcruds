
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
	@mkdir -p api/metadata api/replication api/log
	protoc --proto_path=api metadata.proto \
		--go_out=api/metadata \
		--go-grpc_out=api/metadata \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative
	protoc --proto_path=api replication.proto \
		--go_out=api/replication \
		--go-grpc_out=api/replication \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative
	protoc --proto_path=api log.proto \
		--go_out=api/log \
		--go-grpc_out=api/log \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative
