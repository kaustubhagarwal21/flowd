# flowd

flowd is a durable workflow engine written in Go. You submit a workflow (a
DAG of steps) over a JSON REST API. flowd runs the steps on a pool of worker
goroutines, retrying failures with backoff, and keeps all state in PostgreSQL.
If a worker crashes, another instance picks up its steps.

## Features

- **Workflows as DAGs.** Steps declare `depends_on`. Validation rejects
  unknown dependencies, duplicates, self-dependencies and cycles, and fills in
  defaults.
- **Step types.**
  - `http` calls an endpoint and treats any 2xx as success.
  - `noop` optionally sleeps, which is useful for demos and tests.
- **Retries.** Each step has a timeout and a retry policy. Retries use
  exponential backoff with full jitter.
  - 408, 429, 5xx and network errors are retried.
  - Any other 4xx fails the step immediately.
- **Durable state.** Workflows, runs and steps live in PostgreSQL. Several
  flowd processes can share one database.
- **Leased claims.** A worker claims a runnable step with
  `SELECT ... FOR UPDATE SKIP LOCKED` and holds a time-limited lease, renewed by
  a heartbeat.
- **Fencing.** Every later write for that step must present the claim's owner
  and attempt, so a worker that lost its lease cannot overwrite the result of
  the worker that took over.
- **Failure handling.** A step that fails for good marks its dependents
  `skipped` and the run `failed`. Cancelling a run stops its pending steps, and
  its running steps stop at their next heartbeat.
- **Operations.**
  - Errors are RFC 7807 `application/problem+json`, and `api/openapi.yaml`
    describes the API (OpenAPI 3.0).
  - `/healthz` and Prometheus `/metrics` endpoints.
  - Graceful shutdown on SIGINT and SIGTERM.
  - A distroless Docker image.

## Guarantees and their limits

- **At-least-once.** If a worker dies mid-step, the step runs again after the
  lease expires, so a step can run more than once.
- **Idempotency keys.** Every http call carries
  `Idempotency-Key: <run_id>/<step_id>`, which stays the same across retries
  and re-runs, so the target can recognise and ignore a repeat.
- **No double-running without failures.** Without crashes or expired leases,
  each step runs once, even with several flowd instances.
- **SSRF caution.** The http executor calls whatever URL a workflow names.
  Don't expose the API to untrusted users without restricting targets.

## Quick start

```sh
docker compose up --build        # PostgreSQL 16 + flowd on http://localhost:8080
```

Create a workflow, start a run and check it:

```sh
curl -s -X POST localhost:8080/v1/workflows -H 'Content-Type: application/json' -d '{
  "name": "demo",
  "steps": [
    {"id": "fetch",  "type": "http", "http": {"method": "GET", "url": "https://example.com"}},
    {"id": "wait",   "type": "noop", "noop": {"sleep_ms": 500}, "depends_on": ["fetch"]},
    {"id": "notify", "type": "noop", "depends_on": ["wait"]}
  ]}'
# 201 Created, with the workflow's id

curl -s -X POST localhost:8080/v1/workflows/<workflow-id>/runs -H 'Content-Type: application/json' -d '{}'
# 202 Accepted: {"run_id":"..."}

curl -s localhost:8080/v1/runs/<run-id>
# the run and each step's status, attempt, error and output
```

## API

| Method and path | What it does |
|---|---|
| `POST /v1/workflows` | Validate and store a workflow (201; 400 with the invalid field) |
| `GET /v1/workflows/{id}` | Fetch a workflow |
| `POST /v1/workflows/{id}/runs` | Start a run, with optional `{"input": ...}` (202) |
| `GET /v1/runs/{id}` | A run with its steps |
| `GET /v1/runs?workflow_id=&status=&limit=&cursor=` | List runs, newest first (keyset pagination) |
| `POST /v1/runs/{id}/cancel` | Cancel a running run (409 if it already finished) |
| `GET /healthz`, `GET /metrics` | Health check and Prometheus metrics |

## Configuration

| Flag | Environment | Default |
|---|---|---|
| `-addr` | `FLOWD_ADDR` | `:8080` |
| `-database-url` | `FLOWD_DATABASE_URL` | required |
| `-workers` | `FLOWD_WORKERS` | `8` |
| `-lease` | `FLOWD_LEASE` | `30s` (renewed every lease/3) |
| `-poll` | `FLOWD_POLL` | `200ms` |

## Testing

```sh
export FLOWD_TEST_DATABASE_URL='postgres://user@localhost:5432/db?sslmode=disable'
go test -race ./...      # or: make race
```

Each PostgreSQL test runs in its own throwaway schema. Without
`FLOWD_TEST_DATABASE_URL`, those tests are skipped. The suites cover:

- **Store:**
  - concurrent claimers never share a step;
  - stale owners are fenced out;
  - expired leases are re-claimed;
  - exhausted attempts fail the run and skip its dependents;
  - pagination is stable.
- **Engine:**
  - chains run in order, and the middle steps of a diamond overlap;
  - flaky steps succeed after retries, and timeouts count as failures;
  - cancellation works mid-run;
  - after a crash, another instance re-runs the step with the same
    Idempotency-Key;
  - three instances share 200 runs (1,000 steps), and every step runs exactly
    once.
- **API:** every endpoint, every error status, and the problem+json format.

CI runs gofmt, go vet, staticcheck and `go test -race` against PostgreSQL 16,
and builds the Docker image.

## License

MIT; see [LICENSE](LICENSE).
