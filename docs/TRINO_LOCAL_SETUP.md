# Local Trino Setup Guide

This guide covers how to set up Trino locally for development with the DJ extension. If you already have Trino running (cloud or on-premises), return to the [main setup guide](SETUP.md#trino-setup) instead.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Install](#install)
   - [Using Docker](#using-docker)
   - [Verify](#verify)
   - [Configure](#configure)
3. [Troubleshooting](#troubleshooting)

## Prerequisites

- **Docker** (recommended for easy setup)

## Install

### Using Docker

This guide uses Docker to run Trino locally. If you prefer other installation methods, refer to the [official Trino installation guide](https://trino.io/docs/current/installation.html).

```bash
# Pull and run Trino
docker pull trinodb/trino
docker run --name trino -d -p 8080:8080 trinodb/trino
```

### Verify

```bash
# Test Trino server directly
curl http://localhost:8080/v1/info

# Test with Trino CLI (Requires CLI to be installed. See main setup guide)
trino-cli --server localhost:8080 --execute "SHOW CATALOGS;"
```

Or access the Trino web UI at [http://localhost:8080](http://localhost:8080) (enter any username).

### Configure

Once Trino is running locally, configure your environment variables:

```bash
# Add to your shell profile (.bashrc, .zshrc, etc.)
export TRINO_HOST=localhost
export TRINO_PORT=8080
export TRINO_USERNAME=admin
export TRINO_CATALOG=example
export TRINO_SCHEMA=jaffle_shop
```

## Troubleshooting

### Connection refused

```bash
# Check if Trino is running
curl http://localhost:8080/v1/info

# Check if Docker container exists and is running
docker ps -a | grep trino

# Start the container if it's stopped
docker start trino

# Restart Trino if needed
docker restart trino

# Check logs if there are issues
docker logs trino
```

#### Port already in use

```bash
# Check if port 8080 is available
lsof -i :8080

# Use a different port if port 8080 is already in use
docker run --name trino -d -p 8081:8080 trinodb/trino

# Update your environment variables
export TRINO_PORT=8081
```

---

_Return to the [main setup guide](SETUP.md) to continue with the extension configuration._
