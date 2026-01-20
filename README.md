# Beyond CRUDs

This repository contains the source code for tutorials from [beyondcruds.com](https://beyondcruds.com). Each directory contains a complete implementation of the system covered in its corresponding tutorial.

## Tutorials

| Directory | Tutorial | Description |
|-----------|----------|-------------|
| `mgateway/` | [Building an API Gateway](https://beyondcruds.com/book/chapter1/introduction) | A lightweight API Gateway in Go with routing, load balancing, and middleware |
| `mredis/` | Redis Protocol Implementation | Build a Redis-compatible server from scratch |
## Structure

Each tutorial directory is a standalone Go module with its own:
- Source code implementing the system
- Configuration files
- Tests
- Makefile for building and running

## Getting Started

Navigate to any tutorial directory and follow the README or the corresponding article on beyondcruds.com.

```bash
# Example: Build and run the API Gateway
cd mgateway
make build
./bin/gateway --config-path ./config.yaml
```

## About Beyond CRUDs

Beyond CRUDs teaches you how to build real infrastructure systems from scratch. Instead of just using libraries and frameworks, you'll understand how they work by implementing them yourself.

Topics include:
- API Gateways
- Database protocols
- Distributed systems
- Storage engines
- And more...

Visit [beyondcruds.com](https://beyondcruds.com) for the full tutorials with step-by-step explanations.
