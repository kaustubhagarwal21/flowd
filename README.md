# flowd

flowd is a small workflow engine written in Go that keeps all of its state in
PostgreSQL. A workflow is a DAG of steps described in JSON. You store it and
start runs of it through a REST API. flowd executes each run's steps in
dependency order on a pool of worker goroutines, retries failed steps with
exponential backoff, and records every attempt in the database. Because the
state lives in PostgreSQL, a run survives a crash of the flowd process, and
several flowd processes can share one database and split the work.

## Why

Automation jobs such as a deploy pipeline, provisioning a service or sending
notifications are mostly a series of calls to other services. The calls must
happen in the right order. They must be retried when a service is briefly
down, and they must not be lost when the machine running them restarts. A
plain script run from cron has none of this built in. flowd provides these
pieces in about 3,000 lines of Go, not counting tests:

- **Ordering.** A step runs only after the steps it depends on have
  succeeded. Independent steps run in parallel.
- **Retries.** Each attempt has a timeout. Failed attempts are retried with
  exponential backoff and full jitter. Retryable errors (timeouts, 5xx) are
  kept apart from permanent ones (a 400).
- **Durability.** Every state change is a PostgreSQL transaction. If a worker
  dies, another worker picks up its step once the step's lease expires.
- **A record.** Each step keeps its status, attempt count, last error, output
  and timestamps.

What flowd guarantees, and where the guarantees end, is stated below under
[Guarantees and limits](#guarantees-and-limits).

## Architecture

```
          REST clients (curl, CI jobs, other services)
                             |
                             | JSON over HTTP
                             v
+------------------------------------------------------------------+
| flowd process                                                    |
|                                                                  |
|  internal/api -------------- Wake() ------------+                |
|  handlers, problem+json,                        |                |
|  /healthz, /metrics                             v                |
|       |                               internal/engine            |
|       | create, get, list,            N worker goroutines:       |
|       | cancel                        claim -> execute -> record |
|       |                               plus one heartbeat         |
|       |                               goroutine per running step |
|       v                                  |              |        |
|  internal/store/pgstore <----------------+              v        |
|  SQL: claim (SKIP LOCKED),                   internal/executor   |
|  heartbeat, complete, fail, cancel           http, noop          |
+-------|-------------------------------------------------|--------+
        v                                                 v
  PostgreSQL                                        HTTP targets
  workflows, runs, steps                            (every call carries
  (shared by every flowd instance)                  Idempotency-Key)
```

How a run executes:

1. `POST /v1/workflows/{id}/runs` inserts the run and one row per step in a
   single transaction. Steps without dependencies start `ready` and the rest
   start `pending`. The API then wakes the local engine.
2. An idle worker claims the step that has been runnable the longest. One SQL
   statement (`UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)`)
   picks the step, marks it `running`, increments its `attempt` and gives the
   worker a lease: `lease_owner` and `lease_expires_at`.
3. The worker runs the step under its timeout. Meanwhile a heartbeat
   goroutine extends the lease every lease/3.
4. The worker records the outcome in one transaction. Every such write is
   fenced on `(lease_owner, attempt)`, so it only succeeds while the worker
   still holds the step.
   - On success, the step becomes `succeeded`, and each dependent whose
     dependencies have now all succeeded becomes `ready`.
   - A retryable failure with attempts left puts the step back to `ready`,
     claimable again after a backoff.
   - Any other failure marks the step `failed`, and the run is `failed` at
     once (fail-fast). Every step of the run that is still `pending` or
     `ready` becomes `skipped`: not only the failed step's dependents, but
     also independent branches and steps waiting for a retry. Steps that
     are already `running` are not stopped. Their results are recorded, but
     a failure is not retried and a success makes no other step `ready`.
5. The transaction that completes the last step marks the run `succeeded`.

Step states:

```
                all dependencies               claimed by a worker
  pending ------- succeeded -------> ready ------ (attempt + 1) ------> running
                                       ^                                   |
                                       |  failed with attempts left:       |
                                       +--- claimable again after backoff -+
                                                                           |
                                        succeeded <--- completed ----------+
                                        failed    <--- failed for good ----+
                                                       (the run fails; its
                                                        pending and ready
                                                        steps are skipped)

  Cancelling a run moves its pending, ready and running steps to cancelled.
  A running step whose lease expires is claimed again (attempt + 1), or is
  failed if that was its last attempt.
```

## Quick start

### With Docker Compose

```sh
git clone https://github.com/kaustubhagarwal21/flowd
cd flowd
docker compose up --build
```

This starts PostgreSQL 16 and flowd, and publishes the API on
`127.0.0.1:8080` only, because the API has no authentication.

### Without Docker

You need Go 1.27 or newer and an empty PostgreSQL database. CI and the tests
use PostgreSQL 16. flowd creates its tables when it starts.

```sh
export FLOWD_DATABASE_URL='postgres://user:password@localhost:5432/flowd?sslmode=disable'
go run ./cmd/flowd        # or: make run
```

### A walkthrough with curl

These commands were run against a local flowd on port 8080. The outputs are
real but shortened. Some headers and fields are left out, and `...` marks
text that was cut. JSON marked "formatted" was pretty-printed.

Create a workflow. `check` calls flowd's own health endpoint, `build` and
`test` run in parallel after it, and `test` fails twice on purpose to show
retries:

```sh
curl -si -X POST localhost:8080/v1/workflows -H 'Content-Type: application/json' -d '{
  "name": "deploy-demo",
  "steps": [
    {"id": "check", "type": "http",
     "http": {"method": "GET", "url": "http://localhost:8080/healthz"}},
    {"id": "build", "type": "noop", "noop": {"sleep_ms": 300}, "depends_on": ["check"]},
    {"id": "test", "type": "noop", "noop": {"fail_times": 2},
     "retry": {"max_attempts": 3, "initial_backoff_ms": 100}, "depends_on": ["check"]},
    {"id": "notify", "type": "noop", "depends_on": ["build", "test"]}
  ]
}'
```
```
HTTP/1.1 201 Created
Content-Type: application/json
Location: /v1/workflows/12c94943-d9ec-46de-a0a8-537ee0b4f2dd

{                                                     (formatted, shortened)
  "id": "12c94943-d9ec-46de-a0a8-537ee0b4f2dd",
  "name": "deploy-demo",
  "definition": {"name": "deploy-demo", "steps": [
    {"id": "check", "type": "http",
     "http": {"method": "GET", "url": "http://localhost:8080/healthz"},
     "retry": {"max_attempts": 3, "initial_backoff_ms": 200, "max_backoff_ms": 30000},
     "timeout_ms": 30000},
    ...]},
  "created_at": "2026-09-30T15:38:13.881521Z"
}
```

The stored definition has every default filled in. Start a run:

```sh
WF=12c94943-d9ec-46de-a0a8-537ee0b4f2dd      # the id from the response
curl -si -X POST localhost:8080/v1/workflows/$WF/runs -H 'Content-Type: application/json' \
  -d '{"input": {"env": "staging"}}'
```
```
HTTP/1.1 202 Accepted
Content-Type: application/json
Location: /v1/runs/b37b7212-892e-4533-a284-3ee8e51a9f1d

{"run_id":"b37b7212-892e-4533-a284-3ee8e51a9f1d"}
```

The answer is 202 because the run has only been queued. Poll it:

```sh
RUN=b37b7212-892e-4533-a284-3ee8e51a9f1d
curl -s localhost:8080/v1/runs/$RUN
```
```
{                                                     (formatted, shortened)
  "id": "b37b7212-892e-4533-a284-3ee8e51a9f1d",
  "status": "succeeded",
  "input": {"env": "staging"},
  "created_at": "2026-09-30T15:38:13.912718Z",
  "finished_at": "2026-09-30T15:38:14.335804Z",
  "steps": [
    {"step_id": "check", "status": "succeeded", "attempt": 1, "max_attempts": 3,
     "output": {"body": {"status": "ok"}, "status": 200}, ...},
    {"step_id": "build", "status": "succeeded", "attempt": 1, ...},
    {"step_id": "test", "status": "succeeded", "attempt": 3, "max_attempts": 3,
     "last_error": "noop: attempt 2 fails on purpose (fail_times=2)", ...},
    {"step_id": "notify", "status": "succeeded", "attempt": 1, ...}
  ]
}
```

Start a second run, this time without a body (`curl -s -X POST
localhost:8080/v1/workflows/$WF/runs`). Then list the runs, newest first, one
per page. The cursor is opaque. Pass it back as `cursor` to get the next
page:

```sh
curl -s "localhost:8080/v1/runs?workflow_id=$WF&limit=1"
```
```
{                                                     (formatted, shortened)
  "runs": [{"id": "afa0afb7-384a-48d0-a975-e11e03d8fe8c", "status": "succeeded", ...}],
  "next_cursor": "MTc5MDc4MjY5NjA5NDYyNSxhZmEwYWZiNy0zODRhLTQ4ZDAtYTk3NS1lMTFlMDNkOGZlOGM"
}
```

Every error is an RFC 7807 problem. A validation error names the field:

```sh
curl -s -X POST localhost:8080/v1/workflows -H 'Content-Type: application/json' \
  -d '{"name": "loop", "steps": [{"id": "a", "type": "noop", "depends_on": ["b"]},
                                {"id": "b", "type": "noop", "depends_on": ["a"]}]}'
```
```
{                                                     (formatted; HTTP 400, application/problem+json)
  "type": "about:blank",
  "title": "Bad Request",
  "status": 400,
  "detail": "dependency cycle: a -> b -> a (each step depends on the next)",
  "instance": "/v1/workflows",
  "field": "steps[0].depends_on"
}
```

A misspelt field is rejected instead of silently ignored: `"depends_no"` gives
`400` with `"detail": "invalid JSON body: unknown field \"depends_no\""`.

Cancel a run. Here `SLOW_RUN` is a run of a second workflow whose first step
sleeps for 60 s. The run's steps are cancelled at once in the database, and
the worker running the sleeping step stops it at its next heartbeat.
Cancelling a run that already finished gives `409 Conflict`.

```sh
curl -s -X POST localhost:8080/v1/runs/$SLOW_RUN/cancel
```
```
{                                                     (formatted)
  "id": "8bb0bec3-3ea0-44e0-a824-6db8f19e4222",
  "workflow_id": "ba870914-cc9d-43b3-9356-2321bb1b1b85",
  "status": "cancelled",
  "created_at": "2026-09-30T15:38:18.302803Z",
  "finished_at": "2026-09-30T15:38:18.858923Z"
}
```

Metrics are in the Prometheus text format:

```sh
curl -s localhost:8080/metrics | grep '^flowd_'
```
```
flowd_claim_errors_total 0
flowd_runs_finished_total{status="cancelled"} 1
flowd_runs_finished_total{status="succeeded"} 2
flowd_runs_started_total 3
...                                                   (histogram lines left out)
flowd_steps_executed_total{result="cancelled",type="noop"} 1
flowd_steps_executed_total{result="retry",type="noop"} 4
flowd_steps_executed_total{result="success",type="http"} 2
flowd_steps_executed_total{result="success",type="noop"} 6
```

## API

| Method and path | What it does |
|---|---|
| `POST /v1/workflows` | Validate and store a workflow (201 with `Location`; 400 names the invalid field) |
| `GET /v1/workflows/{id}` | Fetch a workflow |
| `POST /v1/workflows/{id}/runs` | Start a run, with an optional `{"input": {...}}` (202 with `Location`) |
| `GET /v1/runs/{id}` | A run with the state of each step, in topological order |
| `GET /v1/runs?workflow_id=&status=&limit=&cursor=` | List runs, newest first, with keyset pagination (limit 1 to 100, default 20) |
| `POST /v1/runs/{id}/cancel` | Cancel a running run (repeating it is harmless; 409 if the run already finished) |
| `GET /healthz` | 200 if the database answers a ping within 2 s, otherwise 503 |
| `GET /metrics` | Prometheus metrics |

Conventions:

- Request bodies must be JSON sent as `application/json` (415 otherwise).
  They are capped at 1 MiB (413), must hold exactly one JSON value, and may
  not contain unknown fields (400).
- Strings in a body must be valid UTF-8 and must not contain the `\u0000`
  escape or an unpaired surrogate escape such as `\ud800` (400). Go's JSON
  parser accepts all three, but PostgreSQL's `jsonb` cannot store them.
- The body of `POST /v1/workflows/{id}/runs` is optional. An empty body
  means no input, however it is sent, including as an empty chunked body.
- Every error, including 404 for an unknown path and 405 (with `Allow`) for
  a wrong method, is `application/problem+json`.
- A 500 never includes internal details. Those go to the log.
- [`api/openapi.yaml`](api/openapi.yaml) describes the API in OpenAPI 3.0.

## Workflow definitions

| Field | Meaning | Default and limit |
|---|---|---|
| `name` | Label for the workflow | optional |
| `steps[].id` | Unique step ID matching `[a-z0-9_-]{1,64}` | required |
| `steps[].type` | `http` or `noop` | required |
| `steps[].depends_on` | IDs of steps that must succeed first | none; at most 32 |
| `http.method`, `http.url` | GET, HEAD, POST, PUT, PATCH or DELETE; an absolute http(s) URL | required for `http` |
| `http.headers`, `http.body` | Request headers and a JSON body. Header names must be HTTP tokens, and values may not contain control characters other than tab (such as CR, LF or NUL), because Go's HTTP client refuses to send them. | optional |
| `noop.sleep_ms`, `noop.fail_times` | Sleep, and fail the first N attempts on purpose (for demos and tests) | 0; `sleep_ms` at most 600000 |
| `retry.max_attempts` | Attempts in total, including the first | 3; at most 10 |
| `retry.initial_backoff_ms` | Delay ceiling before the first retry | 200; at most `max_backoff_ms` |
| `retry.max_backoff_ms` | Cap on the delay ceiling | 30000; at most 600000 |
| `timeout_ms` | Deadline of each attempt | 30000; at most 600000 |

- A workflow has 1 to 100 steps.
- Validation rejects duplicate IDs, unknown or repeated dependencies,
  self-dependencies and cycles. The error names the field, and for a cycle
  the steps in it (`a -> b -> a`).
- It also rejects a header that Go's HTTP client would refuse to send, and
  names the header. Such a step could never reach its target: each attempt
  would fail the same way until the step ran out of attempts.
- Retry and timeout values above their limit are lowered to the limit. Too
  many steps or dependencies, or too long a `sleep_ms`, is an error.
- There is no shell step type, because running arbitrary commands would be a
  security hole.

An `http` step succeeds on any 2xx. Its output is
`{"status": <code>, "body": <body>}`, keeping the first 16 KiB of the body:
as JSON when PostgreSQL can store it as JSON, otherwise as a string. Network
errors, timeouts, 408, 429 and 5xx are retried. Any other status fails the
step at once, because repeating the same request cannot fix it.

PostgreSQL's `jsonb` type rejects some JSON that Go accepts: invalid UTF-8,
the `\u0000` escape, and unpaired surrogate escapes such as `\ud800`. Step
output that PostgreSQL cannot store as JSON is therefore kept as a JSON
string, with NUL bytes dropped and invalid UTF-8 replaced by U+FFFD. This
applies to the output of every step type. Otherwise saving the result would
fail on every attempt, and a step whose target had succeeded would run
again until its attempts ran out.

## Guarantees and limits

**At-least-once execution.** A step can run more than once:

- its worker crashed or was killed mid-step;
- its worker lost the lease, for example by stalling for longer than the
  lease;
- its result could not be saved;
- an attempt timed out after the target had already acted.

flowd does not promise exactly-once execution. No engine can promise it for
calls to a target that does not cooperate. The gap is the moment after the
target has acted but before flowd has recorded it: a crash there cannot be
told apart from a crash before the call. When no worker crashes, no lease expires and no
attempt fails, each step runs exactly once, even with several flowd
instances. A test checks this: three engines run 1,000 steps.

**Idempotency-Key.** Every http call carries
`Idempotency-Key: <run_id>/<step_id>`. The key is the same for every attempt
of that step in that run, including a re-run after a crash, and user headers
cannot override it. A target that remembers the keys it has processed can
ignore repeats, which turns at-least-once delivery into exactly-once effects.

**Leases and fencing.** A claim is a lease, not a lock held by an open
transaction, so no database connection is tied up while a step runs.

- The lease is stored in the step row and renewed every lease/3. With the
  default 30 s lease, that is every 10 s.
- Leases are set and checked in the database with PostgreSQL's clock
  (`now()`), never with a flowd host's clock.
- A worker that cannot renew its lease, for example while the database is
  unreachable, stops the step when the lease runs out. It times the lease
  on its own clock, from just before the claim or its last successful
  renewal, so the step does not keep running while another instance
  claims it again.
- Every later write for the step must match the claim's `(lease_owner,
  attempt)`, and every claim increments `attempt`. A worker that stalled,
  lost its lease and woke up after another worker took over therefore
  updates zero rows. It then discards its result instead of overwriting the
  newer attempt.
- Fencing protects flowd's own state. It cannot recall an HTTP request that
  a stale worker already sent, which is why the Idempotency-Key exists.

**What a crash or an outage does.**

| Event | What happens |
|---|---|
| flowd is killed (SIGKILL, OOM, power loss) | Its running steps keep their leases until the leases expire (30 s by default). Then any instance claims them again with the next attempt number and the same Idempotency-Key. A step that was on its last attempt is failed with `lease expired: the worker was lost`, and so is its run. |
| SIGTERM or SIGINT | flowd stops accepting requests and stops claiming steps. In-flight steps get up to 10 s to finish and save their results. After that they are cancelled, their results are discarded, and their leases expire as above. |
| PostgreSQL is unreachable | Claims fail. Each worker backs off exponentially with jitter, up to 5 s, and `flowd_claim_errors_total` counts the failures. `/healthz` returns 503, and API calls that need the database return 500. A running step whose lease cannot be renewed is stopped when the lease runs out. A step whose result cannot be saved runs again once its lease expires. |
| flowd freezes or loses its network inside a transaction | Such a transaction may hold the lock on a run. PostgreSQL ends a session that sits idle inside a run-locking transaction for 10 s, which releases the lock. Until then, claims on every instance go on, because they never wait for a run lock. Saving a result of one of that run's steps, or cancelling the run, waits for the lock. |
| A step fails for good (a permanent error, a failed last attempt, or a lease that expired on the last attempt) | The run is `failed` at once (fail-fast). Every step of the run that is still `pending` or `ready` is `skipped`: the failed step's dependents, and also independent branches and steps waiting for a retry. Steps that are already running are not stopped. Their results are recorded, but a failure is not retried and a success makes no other step `ready`. |
| A run is cancelled | Its pending, ready and running steps become `cancelled` in the same transaction. A worker that is running one of them learns this at its next heartbeat, cancels the step's context (which aborts an in-flight HTTP request) and discards the result. |

**Security.** flowd has no authentication. An http step calls whatever URL
its workflow names, including internal addresses such as a cloud metadata
endpoint. Anyone who can create workflows can therefore make flowd send
requests inside your network (server-side request forgery), and read the
answers: a 2xx response body becomes the step's output, the start of any
other response body goes into its `last_error`, and `GET /v1/runs/{id}`
returns both to anyone who can reach the API.

Redirects widen this. The HTTP client follows up to 10 redirects, including
redirects to other hosts, and sends the step's custom headers and its
`Idempotency-Key` to each new location. Go's client withholds credential
headers such as `Authorization` and `Cookie` when a redirect leaves the
original domain, but a custom header such as `X-Api-Key` still goes along.
A restriction on which URLs a step may call must therefore also cover where
the target may redirect.

Run flowd only on a trusted network, and do not expose its API to untrusted
clients. The Compose file publishes it on localhost only.

## Configuration

Each flag falls back to an environment variable, then to its default.

| Flag | Environment | Default | Meaning |
|---|---|---|---|
| `-addr` | `FLOWD_ADDR` | `:8080` | HTTP listen address |
| `-database-url` | `FLOWD_DATABASE_URL` | required | PostgreSQL URL; `pool_max_conns=N` in it sets the pool size (default: the number of CPUs, at least 4) |
| `-workers` | `FLOWD_WORKERS` | `8` | Steps executed at the same time |
| `-lease` | `FLOWD_LEASE` | `30s` | Step lease, renewed every lease/3 |
| `-poll` | `FLOWD_POLL` | `200ms` | Idle wait between empty claims (±20% jitter) |
| `-version` | | | Print the version and exit |

Fixed settings:

- 10 s shutdown grace for in-flight steps and HTTP requests.
- HTTP server timeouts: 5 s to read the request headers, 15 s to read the
  request, 30 s to write the response, 120 s idle.
- Workers wait between failed claims with backoff, up to 5 s.

Logs are JSON lines on stderr (`log/slog`). Requests to `/healthz` and
`/metrics` are logged at debug level only, so that probes do not flood the
log.

Metrics:

| Metric | Labels |
|---|---|
| `flowd_runs_started_total` | |
| `flowd_runs_finished_total` | `status`: succeeded, failed, cancelled |
| `flowd_steps_executed_total` | `type`; `result`: success, retry, failure, lost, cancelled |
| `flowd_step_duration_seconds` (histogram, 1 ms to about 262 s) | `type` |
| `flowd_claim_errors_total` | |

The Go runtime and process collectors are exported as well.

## Testing

```sh
go test ./...                     # PostgreSQL tests are skipped without a database

export FLOWD_TEST_DATABASE_URL='postgres://user:password@localhost:5432/flowd_test?sslmode=disable'
go test -race -count=1 ./...      # everything, with the race detector (or: make race)
make lint                         # gofmt check, go vet and staticcheck
```

Each PostgreSQL test creates a schema with a random name and points its
connections at it through `search_path`. It drops the schema when it ends.
Tests therefore run in parallel on one database without seeing each other's
rows, and the database user needs permission to create schemas.

The suite has 105 test functions (204 subtests) and runs with `-race`. What
each part proves:

- **`internal/workflow`**
  - Table tests cover every validation rule and the message it gives.
  - The topological order is deterministic: Kahn's algorithm, with ties
    going to the step defined first.
  - Cycles are reported by name, and a step downstream of a cycle is not
    named as a member.
  - The header rules are checked against Go's own HTTP client: each header
    in the table is sent through it, and Validate must reject exactly the
    ones the client refuses.
  - Validating an already valid definition does not modify it; a
    concurrent test checks this under `-race`.
- **`internal/store/pgstore`** (against PostgreSQL)
  - Claims come out in order and respect retry times.
  - 16 concurrent claimers never receive the same step (`SKIP LOCKED`).
  - A stale owner or an old attempt is fenced out of heartbeat, complete
    and fail.
  - An expired lease is claimed again with the next attempt, and the old
    owner's late result is rejected.
  - A worker lost on the last attempt fails the run and skips the steps
    that depend on it.
  - Also covered: diamond promotion, retry then permanent failure,
    fail-fast, cancel (including cancel racing a claim), keyset pagination
    that stays stable while runs are being added, concurrent startup
    migrations, and startup that does not wait for other instances' writes.
- **`internal/engine`**
  - Unit tests against an in-memory fake store cover backoff inside the
    full-jitter window, and that permanent errors are not retried.
  - A heartbeat that finds the lease lost or the run cancelled stops the
    step, and nothing is written.
  - Graceful shutdown lets in-flight steps finish, and cancels them after
    the grace period.
  - `Wake` shortens idle polling, and claim errors are counted and backed
    off.
  - The shutdown tests also check that no goroutine is left behind.
  - Eleven behaviour tests run twice, against the fake and against PostgreSQL:
    - a chain runs in order;
    - the middle steps of a diamond overlap in time;
    - a flaky step succeeds on attempt 3;
    - a 400 is not retried and skips the dependent steps;
    - a binary response is stored;
    - a JSON body that jsonb cannot store (a \u0000 escape, a lone surrogate,
      invalid UTF-8) is saved on the first attempt, as text;
    - a timeout counts as a failure;
    - cancelling mid-run stops the running step;
    - a cancel that lands after a step finished but before its result was
      saved is not counted as a lost worker.
  - **Crash recovery:**
    - Engine A is stopped in the middle of an HTTP call without saving
      anything.
    - Engine B claims the step after the lease expires, and the run succeeds
      on attempt 2.
    - The target saw the same Idempotency-Key twice.
  - **Multiple instances:** three engines, each with its own connection
    pool, run 200 runs of 5 steps (1,000 steps). Every step executes exactly
    once, counted both by the executors and by the Idempotency-Keys that
    reach the target.
- **`internal/executor`**
  - http steps: the output format and the 16 KiB cap.
  - The Idempotency-Key is stable across retries and cannot be overridden.
  - Statuses are classified as retryable or permanent. Network errors are
    retryable, and deadlines are respected.
  - noop steps: `fail_times` and `sleep_ms`.
- **`internal/api`** (HTTP tests against a fake store, plus PostgreSQL)
  - Every endpoint's normal path, including a run started with an empty
    chunked body.
  - Errors: 400 (bad JSON, unknown field, trailing data, a validation error
    with its field, bad query parameters or cursor), 404, 405 with `Allow`,
    409, 413, 415 and 503.
  - Bodies with `\u0000`, a lone surrogate escape or invalid UTF-8 get a
    400 and store nothing. Against PostgreSQL they get the same 400 instead
    of failing in the database, while other Unicode, including a surrogate
    pair, is stored and read back unchanged.
  - The shape of every problem+json body.
  - A panic becomes a clean 500, and store errors stay out of responses.
- **`cmd/flowd`**
  - Configuration precedence and errors, and graceful shutdown with a
    request in flight.
  - An end-to-end test runs the real process wiring: PostgreSQL, engine,
    executors, API and HTTP server. It drives a two-step run over HTTP and
    checks that the target received exactly one POST with the step's body
    and `Idempotency-Key: <run_id>/deploy`.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) has two jobs:

- The test job runs gofmt, `go vet`, staticcheck and
  `go test -race -count=1 ./...` against a PostgreSQL 16 service container.
- The docker job builds the image.

## Benchmarks

`cmd/flowd-bench` measures how fast one engine drains a burst of runs.

1. It creates a temporary schema and stores a workflow of 6 noop steps:
   `first`, then 4 parallel steps, then `last`.
2. It starts an engine with W workers in its own process.
3. From a single goroutine, it creates 2,000 runs one after another as fast
   as it can. Each run is one transaction that inserts the run and its 6
   step rows.
4. It waits until every run has finished.

Noop steps do no I/O, so the numbers measure flowd's own cost per step: the
claim, the completion transaction, and the promotion of dependents.

| Workers | Throughput (steps/s) | Wall time | Run latency p50 | Run latency p99 | Time to submit 2,000 runs |
|---|---|---|---|---|---|
| 8 | **1,243.5** (1,108.5 to 1,278.5) | 9.651 s (9.386 to 10.826) | 6.220 s (5.990 to 6.619) | 7.438 s (7.216 to 8.032) | 3.221 s (3.156 to 3.936) |
| 32 | **2,311.2** (2,241.4 to 2,352.2) | 5.192 s (5.102 to 5.354) | 21 ms (20 to 22) | 32 ms (32 to 39) | 5.173 s (5.081 to 5.335) |

Each cell is the median of 3 runs, with the range in parentheses.

- **8 workers.** The engine is the bottleneck. Submitting took about 3 s,
  and draining took about 9.7 s. Runs queue behind earlier runs, so their
  latency is seconds.
- **32 workers.** The engine kept up with the submitting goroutine. Each run
  finished about 21 ms (median) after it was created, and the wall time is
  essentially the time it took to submit the runs. The 32-worker figure is
  therefore the rate at which the benchmark created work. It is a lower
  bound for what 32 workers can drain, not their limit.

How the numbers were taken:

- **Commands.** From the repository root:
  ```sh
  export FLOWD_DATABASE_URL='postgres://kaustubh@127.0.0.1:55432/flowd_test?sslmode=disable'
  go build -o bin/flowd-bench ./cmd/flowd-bench
  ./bin/flowd-bench -runs 2000 -steps 6 -workers 8
  ./bin/flowd-bench -runs 2000 -steps 6 -workers 32
  ```
  `make bench BENCH_FLAGS="-runs 2000 -steps 6 -workers 8"` runs the same
  tool through `go run`.
- **Order.** Runs alternated W=8 and W=32, three of each, with at least 15 s
  between runs.
  - A run started only after 5 s in which no other Go build, Go test or
    `psql` process was running in WSL2.
  - A script checked for such processes every 0.2 s during each run. Two runs
    still overlapped another program's `go build` or `go test`. They were
    discarded and repeated, as the rule said beforehand.
- **What the timing includes.** The time is measured from the first run's
  `created_at` to the last run's `finished_at`, both from the database
  clock. It covers submitting every run (which overlaps with execution) and
  every claim, execution, completion and promotion.
- **What it excludes.** Process start, connecting, creating the schema and
  the workflow, starting the engine, the final report, and dropping the
  schema.
- **Latency.** Each run's `finished_at - created_at`, reported as a
  nearest-rank percentile. Because all runs arrive in one burst, it includes
  the time a run waits behind earlier runs.
- **Hardware.**
  - A laptop with an Intel Core i9-13900HX: 24 cores, 8 performance plus
    16 efficiency, with 32 threads. It was on AC power and had 15.7 GiB of
    RAM visible to Windows 11.
  - Everything ran inside WSL2 (Ubuntu 24.04.3, kernel
    5.15.167.4-microsoft-standard-WSL2).
  - `lscpu` in WSL2 reports 16 cores with 2 threads each (32 CPUs), because
    it does not show the performance/efficiency split. WSL2 had 7.6 GiB of
    memory.
- **Software.** Go 1.27.1 (linux/amd64, GOMAXPROCS=32). The pgx pool had
  its default size of 32 connections, shared by the workers and the
  submitting goroutine.
- **PostgreSQL.**
  - PostgreSQL 16.15 from Ubuntu's package, in a cluster created by
    `initdb`. The data directory is on the WSL2 ext4 disk.
  - The only changed settings are `port=55432`,
    `listen_addresses=127.0.0.1` and `unix_socket_directories=/tmp`, plus
    initdb's locale and time zone.
  - Everything that affects commit cost is at its default:
    `synchronous_commit=on`, `fsync=on`, `wal_sync_method=fdatasync`,
    `shared_buffers=128MB` and `max_connections=100`.

These are numbers for one laptop, not a capacity promise.

Where the time goes:

- **Transactions.** Each step costs two short transactions: the claim and
  the completion. The check for expired steps runs only when a claim finds
  nothing to run, or at most once a second per instance while claims keep
  finding work.
  - PostgreSQL counted about 26,400 committed transactions per 8-worker
    benchmark run (26,426 to 26,467, from `pg_stat_database.xact_commit`).
  - That is 2.2 per step, counting run creation and polling.
- **WAL flushes.** Waiting for the WAL flush is part of the cost, but not
  most of it.
  - Three control runs added `&synchronous_commit=off` to the connection
    URL, so that commits stop waiting for the flush. They followed the same
    rules.
  - With that setting, 8 workers reached 1,679.1 steps/s (median of 3; range
    1,678.4 to 1,713.5).
  - That is about 35% more than with the default setting.

## Project layout

```
cmd/flowd                  the server: flags, wiring, graceful shutdown
cmd/flowd-bench            the benchmark tool
internal/workflow          workflow types, validation, topological order
internal/store             the Store interface and status types
internal/store/pgstore     PostgreSQL implementation and schema.sql
internal/store/pgstore/pgtest  a private schema per test
internal/engine            worker pool, heartbeats, retries and backoff
internal/executor          the http and noop step types
internal/api               REST handlers, problem+json, middleware
internal/metrics           Prometheus collectors
api/openapi.yaml           OpenAPI 3.0 description of the API
```

## Limitations and roadmap

Known limitations:

- **Delivery.** Delivery is at-least-once. Exactly-once effects need targets
  that deduplicate on the Idempotency-Key.
- **Security.** There is no authentication, authorization or TLS, and there
  is no allowlist of http targets (see Security above).
- **Step inputs.** The run input is stored and returned, but the built-in
  step types do not use it yet. Nor can a step read the output of an earlier
  step.
- **Wake-ups across instances.** Creating a run wakes only the instance that
  received the request. Other instances notice new work at their next poll:
  every 200 ms ± 20% by default.
- **Dead steps.** A step whose lease expired on its last attempt is failed,
  and its run with it, by a sweep rather than at the moment the lease
  expires. A worker sweeps when its claim finds nothing to run and, while
  claims keep finding work, at most once a second per instance.
- **A lost result.** If saving a result fails because of a database error
  on a step's last attempt, the step is later failed as "lease expired",
  even though it succeeded.
- **Metrics.**
  - `flowd_runs_finished_total{status="failed"}` can count a run twice when
    two of its running steps fail for good at almost the same time.
  - It does not count runs failed by the lease-expiry check.
  - Two cancel requests that race for the same run can both count it.
- **Data.** Finished runs are never deleted. The schema is one idempotent
  file, not versioned migrations.
- **Containers.** The Compose stack is not tested in CI, although the image
  build is. The flowd container has no healthcheck, because the distroless
  image has no shell or curl.
- **Benchmark.** `flowd-bench` submits from a single goroutine. On the test
  laptop that could not keep 32 workers busy.

Possible next steps:

- LISTEN/NOTIFY to wake every instance when work arrives.
- API tokens and an allowlist of http targets.
- Templating of step requests from the run input and earlier outputs.
- Honor `Retry-After` on 429 and 503.
- A retention job for old runs.
- Versioned migrations.
- A benchmark mode that queues the runs first and then measures only the
  drain.
- Kubernetes manifests.

## License

MIT; see [LICENSE](LICENSE).
