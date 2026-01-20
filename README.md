# Beyond CRUDs

This repository contains production-ready implementations of common backend infrastructure components in Go.

## mgateway - API Gateway

A lightweight, configurable API Gateway built in Go with support for routing, load balancing, and pluggable middleware.

### Features

- **HTTP Routing** - Match requests by method and path
- **Reverse Proxy** - Forward requests to backend services with connection pooling
- **Load Balancing** - Round-robin distribution across multiple backend instances
- **Middleware System** - Pluggable middleware with factory pattern
  - JWT Authentication
  - Rate Limiting (token bucket algorithm)
  - Request/Response Transformation
- **YAML Configuration** - Define routes and middleware via configuration files

### Project Structure

```
mgateway/
├── cmd/gateway/       # Application entry point and CLI
├── config/            # Configuration types
├── router/            # HTTP routing and request dispatch
├── proxy/             # Reverse proxy implementation
├── lb/                # Load balancer
├── middleware/        # Middleware system and implementations
├── ratelimit/         # Token bucket rate limiter
├── transform/         # Request/response transformation
└── tests/             # Test files
```

### Quick Start

```bash
cd mgateway

# Build
make build

# Run with default config
./bin/gateway

# Run with custom options
./bin/gateway --port 9090 --config-path ./config.yaml
```

### Configuration Example

```yaml
routes:
  - id: users_api
    method: GET
    path: /users
    upstream:
      strategy: round_robin
      targets:
        - http://localhost:8082
        - http://localhost:8083
    middlewares:
      - name: auth
        config:
          type: jwt
          secret: "your-secret-key"
      - name: rate_limit
        config:
          type: in_memory
          capacity: 10
          rate: 2
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Host to bind to |
| `--port` | `8081` | Port to listen on |
| `--config-path` | `./config.yaml` | Path to route configuration file |

### Running Tests

```bash
cd mgateway
make test
```

### Architecture

```
Client Request
      │
      ▼
┌─────────────┐
│   Router    │  ← Matches route by method/path
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Middleware  │  ← Auth, Rate Limit, Transform
└──────┬──────┘
       │
       ▼
┌─────────────┐
│Load Balancer│  ← Selects backend (round-robin)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Proxy     │  ← Forwards to backend
└──────┬──────┘
       │
       ▼
   Backend
```

All components implement `http.Handler`, making them composable and testable.
