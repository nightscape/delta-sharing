# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Delta Sharing is an open protocol for secure real-time data exchange. It's a multi-module Scala project with Python bindings that enables organizations to share data regardless of computing platforms.

## Build Systems

The project supports two build systems:
- **SBT** (primary): `./build/sbt`
- **Mill** (alternative): `./mill`

## Common Commands

### Building
```bash
# Compile all modules
./build/sbt compile

# Compile specific module
./build/sbt server/compile
./build/sbt client/compile
./build/sbt spark/compile

# Clean build
./build/sbt clean

# Generate Spark connector JAR
./build/sbt spark/package

# Generate server package
./build/sbt server/universal:packageBin

# Build Docker image
./build/sbt server/docker:publishLocal
```

### Testing
#### MCP (Preferred!)

You can use the `metals` MCP tools to test the application.
Currently, the most important test is the `PropertyTest`.
You can run it via MCP like this:
```
debug-main(mainClass: "io.delta.sharing.server.PropertyTest", module: "server.test", env: {"NAMENODE_HOST":"namenode.localtest.me"}, initialBreakpoints: [...])
```
If you already have a suspicion where the bug could lie, it makes sense to set some `initialBreakpoints` at strategic positions, otherwise the program could already have passed
the interesting places until you have followed up with another tool call.

After starting a `debug-test`, you can inspect the output using the MCP resource
```
metals://debug/{sessionId}/output${suffix}
```
where `{suffix}` can be something like `?outputType=all|stdout|stderr&regex=Exception`.

#### Mill
```bash
# Run all Scala tests
./build/sbt test

# Run specific module tests
./build/sbt server/test
./build/sbt client/test
./build/sbt spark/test

# Run a single test class
./build/sbt "server/testOnly io.delta.sharing.server.DeltaSharingServiceSuite"

# Run tests matching a pattern
./build/sbt "server/testOnly *Suite"

# Run Python tests
./python/dev/pytest

# Run integration tests with Docker
./test.sh
```

### Code Style & Linting
```bash
# Check Scala style (runs automatically during compile)
./build/sbt scalastyle

# Python linting is handled in python/dev/pytest
```

### Python Development
```bash
# Install Python connector in development mode
cd python && pip install -e .

# Run Python tests
./python/dev/pytest

# Generate Python wheel
cd python && python setup.py sdist bdist_wheel
```

### Mill Commands (Alternative Build)
```bash
# Compile
./mill _.compile

# Run tests
./mill _.test

# Run specific test
./mill server.test io.delta.sharing.server.DeltaSharingServiceSuite
```

## Project Architecture

### Module Structure
- **client**: Delta Sharing client library (Scala 2.12/2.13)
  - Core protocol implementation
  - REST client for Delta Sharing API
  - Spark-independent functionality

- **spark**: Apache Spark connector (Scala 2.12/2.13)
  - Depends on client module
  - DataSource implementation for Spark
  - Enables reading shared tables as DataFrames

- **server**: Reference server implementation (Scala 2.13 only)
  - REST API server using Armeria
  - Multi-cloud storage support (S3, Azure, GCS, R2)
  - Authentication providers (Bearer token, OAuth)

- **python**: Python connector
  - Rust-based kernel wrapper
  - Pandas and PySpark integration
  - Separate build system with setup.py

### Key Design Patterns
- Protocol-first design based on REST API
- Provider pattern for cloud storage backends
- Service-based architecture in server module
- Cross-compilation support for multiple Scala versions

### Important Files
- `delta-sharing-protocol-api-description.yml`: REST API specification
- `server/src/main/scala/io/delta/sharing/server/DeltaSharingService.scala`: Main service implementation
- `client/src/main/scala/io/delta/sharing/client/DeltaSharingClient.scala`: Client implementation
- `spark/src/main/scala/io/delta/sharing/spark/DeltaSharingSource.scala`: Spark DataSource

## Environment Variables for Testing

For cloud storage tests:
```bash
# AWS S3
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Azure
export AZURE_STORAGE_ACCOUNT=your_account
export AZURE_STORAGE_KEY=your_key

# Google Cloud
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

## Docker Development

```bash
# Build multi-platform Docker image
./build/sbt server/docker:publishLocal

# Run Docker example
cd examples/docker && docker compose up

# Run Knox example
cd examples/docker-knox && docker compose up
```

## Common Development Tasks

### Adding a New Cloud Storage Provider
1. Implement `CloudFilesReader` trait in server module
2. Add configuration parsing in `DeltaSharingConfig`
3. Register provider in `CloudFilesReaderFactory`
4. Add tests in `server/src/test/scala`

### Running Integration Tests
Integration tests require Docker and test against real cloud storage:
```bash
# Set up environment variables for cloud providers
# Run integration test
./test.sh
```

### Running example Server
1. Run server with debug configuration:
   ```bash
   ./build/sbt "server/run --config delta-sharing-server.yaml"
   ```
2. Server runs on port 9998 by default
3. Check logs in console output

### Debugging through MCP (Preferred!)

You can use the `metals` MCP tools to test the application.
Currently, the only important test is the `server/src/test/scala/io/delta/sharing/server/PropertyTest.scala`.
You can run it via MCP like this:
```
debug-main(mainClass: "io.delta.sharing.server.PropertyTest", module: "server-test", env: {"NAMENODE_HOST":"namenode.localtest.me","TEST_STACK":"knox","KNOX_GATEWAY_URL":"https://knox-gateway.localtest.me:8443/", "SSL_TRUSTSTORES_PATH": "$PWD/examples/docker-knox/truststore"}, initialBreakpoints: [...])
```
If you already have a suspicion where the bug could lie, it makes sense to set some `initialBreakpoints` at strategic positions, otherwise the program could already have passed
the interesting places until you have followed up with another tool call.

After starting a `debug-test`, you can inspect the output using the MCP resource
```
metals://debug/{sessionId}/output${suffix}
```
where `{suffix}` can be something like `?outputType=all|stdout|stderr&regex=Exception`.

Be persistent in using the metals debug tools. I'm still developing them and not everything is working smoothly,
but this is a kind of "Eat your own dogfood" scenario, from which I can learn what is working and what's not.

## Testing Patterns
- Unit tests use ScalaTest with FunSuite
- Mock cloud storage using in-memory implementations
- Integration tests use Docker containers
- Python tests use pytest framework

## Key Dependencies
- Apache Spark: 3.3.2
- Delta Lake Kernel: 3.2.0
- ScalaPB for Protocol Buffers
- Armeria for HTTP server
- Cloud SDKs (AWS, Azure, Google)

## Table Discovery Feature

The server supports dynamic table discovery using regex patterns in addition to static table configuration:

### Static Table Configuration (existing)
```yaml
tables:
- name: "static-table"
  location: "s3://bucket/path/to/table"
  id: "unique-id"
  historyShared: true
```

### Dynamic Table Discovery (new)
```yaml
tables:
- location: "s3://data-lake/team-([^/]+)/table-([^/]+)/_delta_log"
  nameTemplate: "team_${1}_${2}"
  discoveryEnabled: true
  historyShared: true
```

Key features:
- **Regex Patterns**: Use regex in `location` field to match multiple Delta tables
- **Name Templates**: Generate table names using capture groups (`${1}`, `${2}`, etc.)
- **Deterministic IDs**: Automatically generated from table location hash
- **Caching**: Discovery results cached for 5 minutes to avoid repeated filesystem scans
- **Multi-cloud**: Works with S3, GCS, Azure Blob Storage, HDFS

### Implementation Files
- `TableDiscoveryService.scala`: Core discovery logic
- `SharedTableManager.scala`: Integrates discovered tables with static ones
- `ServerConfig.scala`: Extended TableConfig with discovery fields

## Style Guidelines
- Follow Apache Spark Scala Style Guide
- Use ScalaStyle (enforced during compilation)
- Imports should be organized alphabetically
- Use meaningful variable and method names
- Add unit tests for new functionality

## Development Environment Setup
