
GO_CMD := go
BIN := bin
DOCKER_COMPOSE := docker-compose

build: build-mlog build-server build-producer build-consumer

build-mlog:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/mlog .

build-server:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/server ./cmd/server

build-producer:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/producer ./cmd/producer

build-consumer:
	@mkdir -p $(BIN)
	$(GO_CMD) build -o $(BIN)/consumer ./cmd/consumer

run:
	$(GO_CMD) run .

test:
	$(GO_CMD) test -v ./...

compile:
	@mkdir -p api/metadata api/replication api/producer api/consumer api/common api/leader
	protoc --proto_path=api common.proto \
		--go_out=api/common \
		--go-grpc_out=api/common \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative

	protoc --proto_path=api metadata.proto \
		--go_out=api/metadata \
		--go-grpc_out=api/metadata \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative

	protoc --proto_path=api leader.proto \
		--go_out=api/leader \
		--go-grpc_out=api/leader \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative

	protoc --proto_path=api replication.proto \
		--go_out=api/replication \
		--go-grpc_out=api/replication \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative
		
	protoc --proto_path=api producer.proto \
		--go_out=api/producer \
		--go-grpc_out=api/producer \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative

	protoc --proto_path=api consumer.proto \
		--go_out=api/consumer \
		--go-grpc_out=api/consumer \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative
