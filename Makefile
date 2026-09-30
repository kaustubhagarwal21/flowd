# Common tasks. The tests that need PostgreSQL read FLOWD_TEST_DATABASE_URL
# and are skipped when it is unset.

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build test race lint run bench docker

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/flowd ./cmd/flowd

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

# The gofmt check is the same as CI's, so that code passing make lint also
# passes CI. gofmt -l exits 0 even when it lists files, so the list itself
# is tested.
lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "These files need gofmt:"; echo "$$unformatted"; exit 1; fi
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

# Needs FLOWD_DATABASE_URL, e.g. postgres://user:pass@localhost:5432/flowd?sslmode=disable
run: build
	./bin/flowd

# Needs FLOWD_DATABASE_URL. The benchmark works in a temporary schema that it
# drops at exit. Pass flags with BENCH_FLAGS, e.g. BENCH_FLAGS="-runs 500".
bench:
	go run ./cmd/flowd-bench $(BENCH_FLAGS)

docker:
	docker build --build-arg VERSION=$(VERSION) -t flowd:$(VERSION) .
