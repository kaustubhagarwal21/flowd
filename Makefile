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

lint:
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

# Needs FLOWD_DATABASE_URL, e.g. postgres://user:pass@localhost:5432/flowd?sslmode=disable
run: build
	./bin/flowd

bench:
	go test -run='^$$' -bench=. -benchmem ./...

docker:
	docker build --build-arg VERSION=$(VERSION) -t flowd:$(VERSION) .
