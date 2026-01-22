.PHONY: build run test docker-build docker-up docker-down docker-logs docker-clean

GO_CMD := go
BIN := bin
DOCKER_COMPOSE := docker-compose

build:
	@mkdir -p bin
	$(GO_CMD) build -o $(BIN)/gateway .

run:
	$(GO_CMD) run .

test:
	$(GO_CMD) test -v ./...

# Docker commands
# The Docker setup includes:
# - Gateway: API Gateway service (port 8081)
# - OpenTelemetry Collector: Collects metrics and exports to Prometheus (port 8889)
# - Prometheus: Metrics storage and querying (port 9090)
# - Grafana: Metrics visualization (port 3000, default: admin/admin)

docker-build:
	$(DOCKER_COMPOSE) build

docker-up:
	$(DOCKER_COMPOSE) up -d

docker-down:
	$(DOCKER_COMPOSE) down

docker-logs:
	$(DOCKER_COMPOSE) logs -f gateway

docker-logs-all:
	$(DOCKER_COMPOSE) logs -f

docker-restart:
	$(DOCKER_COMPOSE) restart gateway

docker-clean:
	$(DOCKER_COMPOSE) down -v
	docker rmi mgateway-gateway 2>/dev/null || true

# Build and run with Docker (includes Prometheus and Grafana)
docker-run: docker-build docker-up
	@echo "=========================================="
	@echo "Services started successfully!"
	@echo "=========================================="
	@echo "Gateway:        http://localhost:8081"
	@echo "Prometheus UI:  http://localhost:9090"
	@echo "Metrics:        http://localhost:8889/metrics"
	@echo ""
	@echo "View logs with: make docker-logs"
	@echo "Stop services: make docker-down"