# flowd

flowd is a durable workflow engine written in Go. It runs workflows (DAGs of
steps) on PostgreSQL behind a JSON REST API.

This README is a placeholder until the full documentation is written.

## Quick start

    docker compose up --build

The API then listens on http://localhost:8080. The endpoints are described in
[api/openapi.yaml](api/openapi.yaml).

## License

MIT; see [LICENSE](LICENSE).
