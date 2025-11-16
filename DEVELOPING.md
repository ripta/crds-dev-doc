# Local Development Setup

## Prerequisites

- Go 1.24 or later
- PostgreSQL 12 or later
- Docker (optional, for running PostgreSQL)

## Quick Start

1. Start PostgreSQL (using Docker):

```bash
docker run -d --rm \
  --name dev-postgres \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 postgres
```

2. Initialize the database schema:

```bash
psql -h 127.0.0.1 -U postgres -d postgres -a -f schema/crds_up.sql
```

3. Set environment variables:

```bash
export PG_USER=postgres
export PG_PASS=password
export PG_HOST=127.0.0.1
export PG_PORT=5432
export PG_DB=postgres

# Optional: Set custom listen addresses
export DOC_LISTEN_ADDR=:5001      # Web server (default)
export GITTER_LISTEN_ADDR=:5002   # Indexer (default)

# Optional: Enable development mode for template hot-reloading
export IS_DEV=true
```

4. Run the doc server (in one terminal):

```bash
make run-doc
# or: go run -v ./cmd/doc
```

5. Run the gitter indexer (in another terminal):

```bash
make run-gitter
# or: go run -v ./cmd/gitter
```

6. Open http://localhost:5001 in your browser.

## Environment Variables

### Database Configuration

- `CRDS_DEV_STORAGE_DSN` - Full PostgreSQL connection string (overrides individual PG_* vars)

### Service Configuration

- `DOC_LISTEN_ADDR` - Doc server listen address (default: `:5001`)
- `GITTER_LISTEN_ADDR` - Gitter RPC server listen address (default: `:5002`)
- `GITTER_ADDR` - Gitter RPC server address for doc to connect to (default: `127.0.0.1:5002`)
- `IS_DEV` - Set to `"true"` to enable development mode with template hot-reloading (default: empty)
- `GITTER_DRY_RUN` - Set to `"true"` to run gitter in dry-run mode without writing to database (default: empty)

## Running Tests

```bash
make test
```
